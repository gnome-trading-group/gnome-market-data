import * as cdk from "aws-cdk-lib";
import * as pipelines from "aws-cdk-lib/pipelines";
import * as codebuild from "aws-cdk-lib/aws-codebuild";
import * as iam from "aws-cdk-lib/aws-iam";
import * as secrets from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from "constructs";
import { platformRegions, Stage } from "@gnome-trading-group/gnome-shared-cdk";
import { CONFIGS, GITHUB_BRANCH, GITHUB_REPO, MarketDataConfig } from "./config";
import { BackendStack } from "./stacks/backend-stack";
import { StorageStack } from "./stacks/storage-stack";
import { CollectionRegionalStack } from "./stacks/collection-regional-stack";
import { ReconcilerStack } from "./stacks/reconciler-stack";
import { MonitoringStack } from "./stacks/monitoring-stack";

/** Regions shards run in. */
export const COLLECTION_REGIONS = platformRegions();

class AppStage extends cdk.Stage {
  constructor(scope: Construct, id: string, config: MarketDataConfig) {
    super(scope, id, { env: config.account.environment });

    const accountId = config.account.environment.account!;

    const storageStack = new StorageStack(this, "MarketDataStorageStack", {
      config,
    });

    for (const region of COLLECTION_REGIONS) {
      new CollectionRegionalStack(this, `CollectionRegionalStack-${region}`, {
        env: {
          account: accountId,
          region: region,
        },
        config,
        deploymentRegion: region,
        finalBucketName: `gnome-market-data-${config.account.stage}`,
        exchangeRawBucketName: `gnome-market-data-exchange-raw-${config.account.stage}`,
        metadataBucketName: `gnome-market-data-metadata-${config.account.stage}`,
        registryApiKeyId: config.registryApiKeyId,
      });
    }

    const reconcilerStack = new ReconcilerStack(this, "MarketDataReconcilerStack", {
      config,
      regions: COLLECTION_REGIONS,
      rulesTable: storageStack.collectionRulesTable,
      targetsTable: storageStack.collectionTargetsTable,
      shardsTable: storageStack.collectionShardsTable,
      coverageTable: storageStack.collectionCoverageTable,
      gapsTable: storageStack.collectionGapsTable,
      metadataBucket: storageStack.metadataBucket,
    });

    const backendStack = new BackendStack(this, "MarketDataBackendStack", {
      metadataBucket: storageStack.metadataBucket,
      collectionRulesTable: storageStack.collectionRulesTable,
      collectionTargetsTable: storageStack.collectionTargetsTable,
      collectionShardsTable: storageStack.collectionShardsTable,
      collectionCoverageTable: storageStack.collectionCoverageTable,
      collectionGapsTable: storageStack.collectionGapsTable,
      reconcilerQueue: reconcilerStack.wakeQueue,
      collectionRegions: COLLECTION_REGIONS,
      coverageTrackedSinceNs: config.coverageTrackedSinceNs,
    });

    new MonitoringStack(this, "MarketDataMonitoringStack", {
      config,
      api: backendStack.api,
      reconcilerLambda: reconcilerStack.reconcilerLambda,
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
          privileged: true, // Required for Docker layer caching
          environmentVariables: {
            MAVEN_CREDENTIALS: {
              value: githubSecret.secretValue.unsafeUnwrap(),
            },
          }
        },
        cache: codebuild.Cache.local(codebuild.LocalCacheMode.DOCKER_LAYER),
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