import * as cdk from "aws-cdk-lib";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as ecrAssets from 'aws-cdk-lib/aws-ecr-assets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as path from 'path';
import * as fs from 'fs';
import { Construct } from 'constructs';
import { MarketDataConfig } from "../config";

/** CloudWatch metrics namespace for collector metrics */
export const COLLECTOR_METRICS_NAMESPACE = 'GnomeMarketData/Collectors';

/** Metric name for collector log errors */
export const COLLECTOR_ERROR_METRIC_NAME = 'LogErrors';

export interface CollectorRegionalStackProps extends cdk.StackProps {
  config: MarketDataConfig;
  deploymentRegion: string;
  rawBucketName: string;
  primaryEventBus: events.EventBus;
}

export class CollectorRegionalStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;
  public readonly securityGroup: ec2.SecurityGroup;
  public readonly cluster: ecs.Cluster;
  public readonly taskDefinitionFamily: string;
  public readonly taskDefinitionArn: string;
  public readonly collectorLogGroup: logs.LogGroup;
  public readonly deploymentRegion: string;

  constructor(scope: Construct, id: string, props: CollectorRegionalStackProps) {
    super(scope, id, props);

    this.deploymentRegion = props.deploymentRegion;

    this.vpc = new ec2.Vpc(this, 'CollectorEcsVpc', {
      maxAzs: 2,
      natGateways: 0, // Avoid NAT Gateway costs
      subnetConfiguration: [
        {
          name: 'PublicSubnet',
          subnetType: ec2.SubnetType.PUBLIC,
        },
      ],
    });

    this.securityGroup = new ec2.SecurityGroup(this, 'CollectorSecurityGroup', {
      vpc: this.vpc,
      description: 'Security group for collector Fargate tasks',
      allowAllOutbound: true,
    });

    this.collectorLogGroup = new logs.LogGroup(this, 'CollectorEcsLogGroup', {
      logGroupName: `/ecs/collector-${props.deploymentRegion}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      retention: logs.RetentionDays.ONE_WEEK,
    });

    // Metric filter to count errors in the log group
    // Note: dimensions are only supported with JSON filter patterns, so we include region in the metric name
    new logs.MetricFilter(this, 'CollectorErrorMetricFilter', {
      logGroup: this.collectorLogGroup,
      metricNamespace: COLLECTOR_METRICS_NAMESPACE,
      metricName: `${COLLECTOR_ERROR_METRIC_NAME}-${props.deploymentRegion}`,
      filterPattern: logs.FilterPattern.anyTerm('ERROR', 'Exception', 'exception', 'Error', 'error', 'UNKNOWN_ERROR'),
      metricValue: '1',
    });

    const taskRole = new iam.Role(this, 'EcsTaskRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    });

    const rawBucket = s3.Bucket.fromBucketName(this, 'RawBucket', props.rawBucketName);
    rawBucket.grantReadWrite(taskRole);

    this.cluster = new ecs.Cluster(this, 'CollectorEcsCluster', { 
      clusterName: `CollectorCluster-${props.deploymentRegion}`,
      vpc: this.vpc,
    });

    this.taskDefinitionFamily = `CollectorTaskDefinition-${props.deploymentRegion}`;
    const taskDefinition = new ecs.FargateTaskDefinition(this, 'CollectorTaskDefinition', {
      family: this.taskDefinitionFamily,
      taskRole,
      memoryLimitMiB: 512,
      cpu: 256,
    });

    const dockerImage = new ecrAssets.DockerImageAsset(this, 'JavaAppImage', {
      directory: this.buildDockerfile(props.config.collectorOrchestratorVersion),
      buildSecrets: {
        MAVEN_CREDENTIALS: 'env=MAVEN_CREDENTIALS',
      },
    });

    taskDefinition.addContainer('CollectorContainer', {
      image: ecs.ContainerImage.fromDockerImageAsset(dockerImage),
      portMappings: [{ containerPort: 8080 }],
      environment: {
        MAIN_CLASS: 'group.gnometrading.collectors.DelegatingCollectorOrchestrator',
        OUTPUT_BUCKET: props.rawBucketName,
        STAGE: props.config.account.stage,
      },
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: 'collector',
        logGroup: this.collectorLogGroup,
      }),
      memoryLimitMiB: 512,
      cpu: 256,
      // Allow up to 75 seconds for graceful shutdown to complete the current minute file
      stopTimeout: cdk.Duration.seconds(75),
    });

    this.taskDefinitionArn = taskDefinition.taskDefinitionArn;

    // Forward ECS task state changes to the primary region's event bus
    const ecsEventRule = new events.Rule(this, 'EcsTaskStateForwardRule', {
      eventPattern: {
        source: ['aws.ecs'],
        detailType: ['ECS Task State Change'],
        detail: {
          clusterArn: [this.cluster.clusterArn],
          lastStatus: ['RUNNING', 'STOPPED', 'PENDING'],
        },
      },
    });

    ecsEventRule.addTarget(new targets.EventBus(props.primaryEventBus));

    new cdk.CfnOutput(this, 'TaskDefinitionArn', {
      value: this.taskDefinitionArn,
      description: `Collector Task Definition ARN for ${props.deploymentRegion}`,
    });

    new cdk.CfnOutput(this, 'ClusterArn', {
      value: this.cluster.clusterArn,
      description: `Collector ECS Cluster ARN for ${props.deploymentRegion}`,
    });
  }

  private buildDockerfile(orchestratorVersion: string) {
    const dockerDir = path.join(__dirname, `collector-docker-${this.deploymentRegion}`);

    if (!fs.existsSync(dockerDir)) {
      fs.mkdirSync(dockerDir);
    }

    const dockerfileContent = `
      FROM ubuntu:24.04

      RUN apt-get update && apt-get install -y wget jq openjdk-17-jdk

      ARG MAIN_CLASS

      RUN --mount=type=secret,id=MAVEN_CREDENTIALS \
        export MAVEN_CREDENTIALS=$(cat /run/secrets/MAVEN_CREDENTIALS) && \
        MAVEN_USERNAME=$(echo $MAVEN_CREDENTIALS | jq -r \'.GITHUB_ACTOR\') && \
        MAVEN_PASSWORD=$(echo $MAVEN_CREDENTIALS | jq -r \'.GITHUB_TOKEN\') && \
        wget --user=$MAVEN_USERNAME --password=$MAVEN_PASSWORD -O app.jar "https://maven.pkg.github.com/gnome-trading-group/gnome-orchestrator/group/gnometrading/gnome-orchestrator/${orchestratorVersion}/gnome-orchestrator-${orchestratorVersion}.jar"

      RUN echo '#!/bin/sh\\nexec java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -cp app.jar $MAIN_CLASS' > start.sh && chmod +x start.sh

      CMD ["./start.sh"]
    `.trim();

    fs.writeFileSync(path.join(dockerDir, 'Dockerfile'), dockerfileContent);
    return dockerDir;
  }
}

