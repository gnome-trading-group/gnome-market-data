import * as cdk from "aws-cdk-lib";
import * as pipelines from "aws-cdk-lib/pipelines";
import * as codebuild from "aws-cdk-lib/aws-codebuild";
import * as iam from "aws-cdk-lib/aws-iam";
import * as secrets from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from "constructs";
import { Stage } from "@gnome-trading-group/gnome-shared-cdk";
import { CONFIGS, GITHUB_BRANCH, GITHUB_REPO, MarketDataConfig } from "./config";
import { BackendStack, CollectorRegionConfig } from "./stacks/backend-stack";
import { StorageStack } from "./stacks/storage-stack";
import { CollectorRegionalStack } from "./stacks/collector-regional-stack";
import { EventBusStack } from "./stacks/event-bus-stack";
import { MonitoringStack } from "./stacks/monitoring-stack";
import { TransformerStack } from "./stacks/transformer-stack";
import { MergerStack } from "./stacks/merger-stack";
import { GapDetectorStack } from "./stacks/gap-detector-stack";

/** Regions where collectors can be deployed */
export const COLLECTOR_REGIONS = ["us-east-1", "ap-northeast-1"];

class AppStage extends cdk.Stage {
  constructor(scope: Construct, id: string, config: MarketDataConfig) {
    super(scope, id, { env: config.account.environment });

    const accountId = config.account.environment.account!;

    // Create storage stack (includes buckets, tables, and queues with S3 notifications)
    const storageStack = new StorageStack(this, "MarketDataStorageStack", {
      config,
    });

    const eventBusStack = new EventBusStack(this, "MarketDataEventBusStack");

    // Regional collector stacks (VPC, ECS, task definition) - one per region
    const collectorRegionalStacks: Record<string, CollectorRegionalStack> = {};
    for (const region of COLLECTOR_REGIONS) {
      const regionalStack = new CollectorRegionalStack(this, `CollectorRegionalStack-${region}`, {
        env: {
          account: accountId,
          region: region,
        },
        crossRegionReferences: true,
        config,
        deploymentRegion: region,
        rawBucketName: storageStack.rawBucket.bucketName,
        primaryEventBus: eventBusStack.collectorEventBus,
      });
      collectorRegionalStacks[region] = regionalStack;
    }

    const collectorRegions: Record<string, CollectorRegionConfig> = {};
    for (const [region, stack] of Object.entries(collectorRegionalStacks)) {
      collectorRegions[region] = {
        region: region,
        clusterName: stack.cluster.clusterName,
        clusterArn: stack.cluster.clusterArn,
        taskDefinitionFamily: stack.taskDefinitionFamily,
        securityGroupId: stack.securityGroup.securityGroupId,
        subnetIds: stack.vpc.publicSubnets.map(subnet => subnet.subnetId),
        logGroupName: stack.collectorLogGroup.logGroupName,
      };
    }

    const backendStack = new BackendStack(this, "MarketDataBackendStack", {
      crossRegionReferences: true,
      collectorsTable: storageStack.collectorsTable,
      collectorRegions: collectorRegions,
      collectorDeploymentVersion: config.collectorOrchestratorVersion,
      collectorEventBus: eventBusStack.collectorEventBus,
      transformJobsTable: storageStack.transformJobsTable,
      gapsTable: storageStack.gapsTable,
    });

    const transformerStack = new TransformerStack(this, "MarketDataTransformerStack", {
      mergedBucket: storageStack.mergedBucket,
      finalBucket: storageStack.finalBucket,
      transformJobsTable: storageStack.transformJobsTable,
      transformerQueue: storageStack.transformerQueue,
      config,
    });

    const mergerStack = new MergerStack(this, "MarketDataMergerStack", {
      rawBucket: storageStack.rawBucket,
      mergedBucket: storageStack.mergedBucket,
      mergerQueue: storageStack.mergerQueue,
      config,
    });

    const gapDetectorStack = new GapDetectorStack(this, "MarketDataGapDetectorStack", {
      mergedBucket: storageStack.mergedBucket,
      gapsTable: storageStack.gapsTable,
      gapQueue: storageStack.gapQueue,
      config,
    });

    new MonitoringStack(this, "MarketDataMonitoringStack", {
      collectorRegions: COLLECTOR_REGIONS,
      api: backendStack.api,
      gapLambda: gapDetectorStack.gapLambda,
      gapQueue: storageStack.gapQueue,
      mergerLambda: mergerStack.mergerLambda,
      mergerQueue: storageStack.mergerQueue,
      transformerJobCreatorLambda: transformerStack.transformerJobCreatorLambda,
      transformerQueue: storageStack.transformerQueue,
      transformerJobProcessorLambda: transformerStack.transformerJobProcessorLambda,
    });
  }
}

export class MarketDataPipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const npmSecret = secrets.Secret.fromSecretNameV2(this, 'NPMToken', 'npm-token');
    const githubSecret = secrets.Secret.fromSecretNameV2(this, 'GithubMaven', 'GITHUB_MAVEN');
    const dockerHubCredentials = secrets.Secret.fromSecretNameV2(this, 'DockerHub', 'docker-hub-credentials');

    const pipeline = new pipelines.CodePipeline(this, "MarketDataPipeline", {
      crossAccountKeys: true,
      pipelineName: "MarketDataPipeline",
      // Enable Docker for synth - required for Lambda bundling
      dockerEnabledForSynth: true,
      synth: new pipelines.ShellStep("Synth", {
        input: pipelines.CodePipelineSource.gitHub(GITHUB_REPO, GITHUB_BRANCH),
        commands: [
          'echo "//npm.pkg.github.com/:_authToken=${NPM_TOKEN}" > ~/.npmrc',
          "cd cdk/",
          "npm ci",
          "npx cdk synth"
        ],
        env: {
          NPM_TOKEN: npmSecret.secretValue.unsafeUnwrap(),
          MAVEN_CREDENTIALS: githubSecret.secretValue.unsafeUnwrap(),
        },
        primaryOutputDirectory: 'cdk/cdk.out',
      }),
      dockerCredentials: [
        pipelines.DockerCredential.dockerHub(dockerHubCredentials),
      ],
      assetPublishingCodeBuildDefaults: {
        buildEnvironment: {
          buildImage: codebuild.LinuxBuildImage.AMAZON_LINUX_2_5,
          environmentVariables: {
            MAVEN_CREDENTIALS: {
              value: githubSecret.secretValue.unsafeUnwrap(),
            },
          }
        },
      },
      synthCodeBuildDefaults: {
        buildEnvironment: {
          buildImage: codebuild.LinuxBuildImage.AMAZON_LINUX_2_5,
          privileged: true, // Required for Docker builds
          environmentVariables: {
            MAVEN_CREDENTIALS: {
              value: githubSecret.secretValue.unsafeUnwrap(),
            },
          }
        },
        rolePolicy: [
          new iam.PolicyStatement({
            actions: ['sts:AssumeRole'],
            resources: ['*'],
            conditions: {
              StringEquals: {
                'iam:ResourceTag/aws-cdk:bootstrap-role': 'lookup',
              },
            },
          })
        ],
      }
    });

    const dev = new AppStage(this, "Dev", CONFIGS[Stage.DEV]!);
    // const staging = new AppStage(this, "Staging", CONFIGS[Stage.STAGING]!);
    const prod = new AppStage(this, "Prod", CONFIGS[Stage.PROD]!);

    pipeline.addStage(dev);

    pipeline.addStage(prod, {
      pre: [new pipelines.ManualApprovalStep('ApproveProd')],
    });

    pipeline.buildPipeline();
    npmSecret.grantRead(pipeline.synthProject.role!!);
    npmSecret.grantRead(pipeline.pipeline.role);
    githubSecret.grantRead(pipeline.synthProject.role!!);
    githubSecret.grantRead(pipeline.pipeline.role);
  }
}