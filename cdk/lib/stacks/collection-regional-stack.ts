import * as cdk from "aws-cdk-lib";
import * as autoscaling from "aws-cdk-lib/aws-autoscaling";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as ecrAssets from "aws-cdk-lib/aws-ecr-assets";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as path from "path";
import * as fs from "fs";
import { Construct } from "constructs";
import { lookupPlatformNetwork, PRIMARY_REGION } from "@gnome-trading-group/gnome-shared-cdk";
import { MarketDataConfig } from "../config";

export interface CollectionRegionalStackProps extends cdk.StackProps {
  config: MarketDataConfig;
  deploymentRegion: string;
  finalBucketName: string;
  exchangeRawBucketName: string;
  metadataBucketName: string;
  registryApiKeyId: string;
}

// What every shard host runs: 1 vCPU, 8 GB (decision D4; the 6-0 capacity test ran 1,000 streams on one).
const ON_DEMAND_TYPE = "r7a.medium";
// Spot always takes the cheapest pool with capacity (lowest-price): r7a.medium, about $0.022/h against $0.035-0.05/h
// for the 2 vCPU 8 GB types, which are here only as fallbacks for when no r7a.medium is left in a zone. A reclaimed
// research shard is replaced within minutes and its minutes recorded as a gap (decision D5); that is cheaper than
// paying for steadier pools.
const SPOT_TYPES = ["r7a.medium", "m7a.large", "m6a.large"];
const MAX_HOSTS = 40;

/** Names the reconciler (gnome-market-data EcsServices) builds from a region; the two must agree. */
export const collectionNames = (region: string) => ({
  cluster: `CollectionCluster-${region}`,
  onDemandProvider: `collection-ondemand-${region}`,
  spotProvider: `collection-spot-${region}`,
  taskDefinitionFamily: `collection-shard-${region}`,
  taskRole: `gnome-collection-shard-task-${region}`,
  executionRole: `gnome-collection-shard-exec-${region}`,
  logGroup: `/collection/shards-${region}`,
});

/**
 * Where a region's shards run: an ECS cluster in the platform network with an on-demand capacity provider (production
 * shards) and a Spot one (research), both scaling to zero, and the base shard task definition the reconciler copies
 * per shard (adding SHARD_ID and ORCHESTRATOR_VERSION). One shard per host: host networking gives each its own
 * public IP (venue per-IP limits) and private IP (Aeron, Phase 10), and a 6 GiB reservation leaves room for no second.
 */
export class CollectionRegionalStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: CollectionRegionalStackProps) {
    super(scope, id, props);
    const names = collectionNames(props.deploymentRegion);
    const { vpc, securityGroup } = lookupPlatformNetwork(this);

    const cluster = new ecs.Cluster(this, "CollectionCluster", {
      clusterName: names.cluster,
      vpc,
      containerInsightsV2: ecs.ContainerInsights.DISABLED,
    });

    const hostRole = new iam.Role(this, "ShardHostRole", {
      assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonEC2ContainerServiceforEC2Role"),
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMManagedInstanceCore"),
      ],
    });
    const hostTemplate = (id: string, instanceType: string) => new ec2.LaunchTemplate(this, id, {
      instanceType: new ec2.InstanceType(instanceType),
      machineImage: ecs.EcsOptimizedImage.amazonLinux2023(),
      role: hostRole,
      securityGroup,
      requireImdsv2: true,
      userData: ec2.UserData.forLinux(),
      blockDevices: [{ deviceName: "/dev/xvda", volume: ec2.BlockDeviceVolume.ebs(30, { encrypted: true }) }],
    });

    const onDemand = new autoscaling.AutoScalingGroup(this, "OnDemandHosts", {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      launchTemplate: hostTemplate("OnDemandTemplate", ON_DEMAND_TYPE),
      minCapacity: 0,
      maxCapacity: MAX_HOSTS,
      newInstancesProtectedFromScaleIn: true,
    });
    const spot = new autoscaling.AutoScalingGroup(this, "SpotHosts", {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      mixedInstancesPolicy: {
        launchTemplate: hostTemplate("SpotTemplate", SPOT_TYPES[0]),
        launchTemplateOverrides: SPOT_TYPES.map((type) => ({ instanceType: new ec2.InstanceType(type) })),
        instancesDistribution: {
          onDemandBaseCapacity: 0,
          onDemandPercentageAboveBaseCapacity: 0,
          spotAllocationStrategy: autoscaling.SpotAllocationStrategy.LOWEST_PRICE,
        },
      },
      minCapacity: 0,
      maxCapacity: MAX_HOSTS,
      newInstancesProtectedFromScaleIn: true,
      capacityRebalance: true,
    });

    cluster.addAsgCapacityProvider(new ecs.AsgCapacityProvider(this, "OnDemandProvider", {
      capacityProviderName: names.onDemandProvider,
      autoScalingGroup: onDemand,
      enableManagedScaling: true,
      enableManagedTerminationProtection: true,
      targetCapacityPercent: 100,
    }));
    // A Spot interruption notice drains the host: the shard gets SIGTERM and flushes, the reconciler places its
    // streams elsewhere, and the minutes until then are a marked gap (decision D5).
    cluster.addAsgCapacityProvider(new ecs.AsgCapacityProvider(this, "SpotProvider", {
      capacityProviderName: names.spotProvider,
      autoScalingGroup: spot,
      enableManagedScaling: true,
      enableManagedTerminationProtection: true,
      targetCapacityPercent: 100,
      spotInstanceDraining: true,
    }));

    const taskRole = new iam.Role(this, "ShardTaskRole", {
      roleName: names.taskRole,
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
    });
    // Minute files go straight to final, written once each (a conditional put; the first writer keeps a minute).
    for (const bucket of [props.finalBucketName, props.exchangeRawBucketName]) {
      taskRole.addToPolicy(new iam.PolicyStatement({
        actions: ["s3:PutObject", "s3:AbortMultipartUpload"],
        resources: [`arn:aws:s3:::${bucket}/*`],
      }));
    }
    taskRole.addToPolicy(new iam.PolicyStatement({
      actions: ["s3:PutObject"],
      resources: [`arn:aws:s3:::${props.metadataBucketName}/collection/shards/*`],
    }));
    // The shard's assignment, in the control plane's region whatever region the shard runs in.
    taskRole.addToPolicy(new iam.PolicyStatement({
      actions: ["dynamodb:GetItem"],
      resources: [`arn:aws:dynamodb:${PRIMARY_REGION}:${this.account}:table/collection-shards`],
    }));
    taskRole.addToPolicy(new iam.PolicyStatement({
      actions: ["apigateway:GET"],
      resources: [`arn:aws:apigateway:${PRIMARY_REGION}::/apikeys/${props.registryApiKeyId}`],
    }));
    // The market-data accounts' keys, and the token that downloads the orchestrator release. Never gnome/accounts/*.
    taskRole.addToPolicy(new iam.PolicyStatement({
      actions: ["secretsmanager:GetSecretValue"],
      resources: [
        `arn:aws:secretsmanager:*:${this.account}:secret:gnome/market-data/*`,
        `arn:aws:secretsmanager:*:${this.account}:secret:gnomepy/gh-token*`,
      ],
    }));
    const executionRole = new iam.Role(this, "ShardExecutionRole", {
      roleName: names.executionRole,
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"),
      ],
    });

    const logGroup = new logs.LogGroup(this, "ShardLogGroup", {
      logGroupName: names.logGroup,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const taskDefinition = new ecs.Ec2TaskDefinition(this, "ShardTaskDefinition", {
      family: names.taskDefinitionFamily,
      networkMode: ecs.NetworkMode.HOST,
      taskRole,
      executionRole,
    });
    taskDefinition.addContainer("ShardContainer", {
      containerName: "ShardContainer",
      image: ecs.ContainerImage.fromDockerImageAsset(new ecrAssets.DockerImageAsset(this, "ShardImage", {
        directory: this.ensureDockerfile(),
        platform: ecrAssets.Platform.LINUX_AMD64,
      })),
      // A soft reservation over half a host's memory: one shard per host, and the JVM sizes its heap (35%) from the
      // host's memory, as in the capacity test.
      memoryReservationMiB: 6 * 1024,
      cpu: 1024,
      environment: {
        MAIN_CLASS: "group.gnometrading.collectors.ShardOrchestrator",
        OUTPUT_BUCKET: props.finalBucketName,
        RAW_CAPTURE_BUCKET: props.exchangeRawBucketName,
        STATUS_BUCKET: props.metadataBucketName,
        STAGE: props.config.account.stage,
        REGISTRY_API_KEY_ID: props.registryApiKeyId,
      },
      healthCheck: {
        command: ["CMD-SHELL", "wget --spider --quiet http://localhost:8080/health || exit 1"],
        interval: cdk.Duration.seconds(15),
        timeout: cdk.Duration.seconds(5),
        retries: 3,
        startPeriod: cdk.Duration.seconds(120),
      },
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: "shard",
        logGroup,
        // In blocking mode a slow CloudWatch stalls stdout writes; lines past the driver's buffer are dropped instead.
        mode: ecs.AwsLogDriverMode.NON_BLOCKING,
      }),
      // On SIGTERM the shard stops its streams, flushes every partial minute and drains the uploader within 60 + 45 s.
      stopTimeout: cdk.Duration.seconds(120),
    });
  }

  private ensureDockerfile() {
    // Every region shares one Dockerfile and start script, written out at synth time.
    const dockerDir = path.join(__dirname, "shard-docker");
    if (!fs.existsSync(dockerDir)) {
      fs.mkdirSync(dockerDir);
    }
    fs.writeFileSync(path.join(dockerDir, "Dockerfile"), `
FROM ubuntu:24.04

# wget is not used by start.sh: the ECS container health check runs it.
RUN apt-get update && apt-get install -y curl wget jq openjdk-17-jdk python3-boto3 && rm -rf /var/lib/apt/lists/*

COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]
`.trim());
    fs.copyFileSync(path.join(__dirname, "shard-start.sh"), path.join(dockerDir, "start.sh"));
    return dockerDir;
  }
}
