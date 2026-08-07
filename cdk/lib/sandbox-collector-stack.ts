import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as budgets from 'aws-cdk-lib/aws-budgets';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecrAssets from 'aws-cdk-lib/aws-ecr-assets';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

/**
 * Isolated, parameterized collector for a personal development account.
 *
 * This stack deliberately does not depend on the organization's pipeline,
 * shared infrastructure, Cognito pool, or Dev/Prod accounts.
 */
export class SandboxCollectorStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: cdk.StackProps) {
    super(scope, id, props);

    const listingId = new cdk.CfnParameter(this, 'ListingId', {
      type: 'String',
      default: '',
      allowedPattern: '^$|^[0-9]+$',
      description: 'Legacy single Gnome registry listing ID. Ignored when ListingIds is provided.',
    });
    const listingIds = new cdk.CfnParameter(this, 'ListingIds', {
      type: 'String',
      default: '',
      allowedPattern: '^$|^[0-9]+( *, *[0-9]+)*$',
      description: 'Comma-separated Gnome registry listing IDs collected concurrently in one task.',
    });
    const hasListingIds = new cdk.CfnCondition(this, 'HasListingIds', {
      expression: cdk.Fn.conditionNot(cdk.Fn.conditionEquals(listingIds.valueAsString, '')),
    });
    const configuredListings = cdk.Token.asString(
      cdk.Fn.conditionIf(hasListingIds.logicalId, listingIds.valueAsString, listingId.valueAsString),
    );
    new cdk.CfnRule(this, 'RequireListingSelection', {
      assertions: [{
        assert: cdk.Fn.conditionOr(
          cdk.Fn.conditionNot(cdk.Fn.conditionEquals(listingIds.valueAsString, '')),
          cdk.Fn.conditionNot(cdk.Fn.conditionEquals(listingId.valueAsString, '')),
        ),
        assertDescription: 'Provide ListingIds or the legacy ListingId parameter.',
      }],
    });
    const registryUrl = new cdk.CfnParameter(this, 'RegistryUrl', {
      type: 'String',
      description: 'Gnome registry API hostname, without a protocol or trailing slash.',
    });
    const registryApiKeySecretArn = new cdk.CfnParameter(this, 'RegistryApiKeySecretArn', {
      type: 'String',
      description: 'ARN of an existing Secrets Manager secret containing only the registry API key.',
    });
    const desiredCount = new cdk.CfnParameter(this, 'DesiredCount', {
      type: 'Number',
      default: 0,
      allowedValues: ['0', '1'],
      description: 'Keep at 0 during setup. Set to 1 only after deployment approval.',
    });
    const rawRetentionDays = new cdk.CfnParameter(this, 'RawRetentionDays', {
      type: 'Number',
      default: 30,
      minValue: 7,
      maxValue: 365,
      description: 'Days to retain normalized and lossless raw capture objects.',
    });
    const budgetAlertEmail = new cdk.CfnParameter(this, 'BudgetAlertEmail', {
      type: 'String',
      noEcho: true,
      description: 'Email address for account-wide sandbox cost alerts.',
    });

    const budgetSubscriber: budgets.CfnBudget.SubscriberProperty = {
      address: budgetAlertEmail.valueAsString,
      subscriptionType: 'EMAIL',
    };
    new budgets.CfnBudget(this, 'SandboxMonthlyBudget', {
      budget: {
        budgetLimit: { amount: 100, unit: 'USD' },
        budgetName: 'gnome-polymarket-sandbox',
        budgetType: 'COST',
        costTypes: {
          includeCredit: false,
          includeRefund: false,
        },
        timeUnit: 'MONTHLY',
      },
      notificationsWithSubscribers: [10, 25, 50, 100].map(threshold => ({
        notification: {
          comparisonOperator: 'GREATER_THAN',
          notificationType: 'ACTUAL',
          threshold,
          thresholdType: 'ABSOLUTE_VALUE',
        },
        subscribers: [budgetSubscriber],
      })),
    });

    const bucketDefaults: s3.BucketProps = {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      lifecycleRules: [{ expiration: cdk.Duration.days(rawRetentionDays.valueAsNumber) }],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    };
    const normalizedRawBucket = new s3.Bucket(this, 'NormalizedRawBucket', bucketDefaults);
    const venueRawBucket = new s3.Bucket(this, 'VenueRawBucket', bucketDefaults);

    const vpc = new ec2.Vpc(this, 'CollectorVpc', {
      availabilityZones: ['us-east-1a', 'us-east-1b'],
      natGateways: 0,
      subnetConfiguration: [{
        name: 'Public',
        subnetType: ec2.SubnetType.PUBLIC,
      }],
    });
    const securityGroup = new ec2.SecurityGroup(this, 'CollectorSecurityGroup', {
      vpc,
      allowAllOutbound: true,
      description: 'Outbound-only network access for the Polymarket sandbox collector.',
    });
    const cluster = new ecs.Cluster(this, 'CollectorCluster', { vpc });
    const logGroup = new logs.LogGroup(this, 'CollectorLogGroup', {
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'CollectorTaskDefinition', {
      cpu: 256,
      memoryLimitMiB: 512,
      runtimePlatform: {
        cpuArchitecture: ecs.CpuArchitecture.X86_64,
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
      },
    });
    normalizedRawBucket.grantPut(taskDefinition.taskRole);
    venueRawBucket.grantPut(taskDefinition.taskRole);

    const image = new ecrAssets.DockerImageAsset(this, 'CollectorImage', {
      directory: path.join(__dirname, '../docker/sandbox-collector'),
      platform: ecrAssets.Platform.LINUX_AMD64,
    });
    const registryApiKey = secretsmanager.Secret.fromSecretCompleteArn(
      this,
      'RegistryApiKey',
      registryApiKeySecretArn.valueAsString,
    );

    taskDefinition.addContainer('CollectorContainer', {
      image: ecs.ContainerImage.fromDockerImageAsset(image),
      cpu: 256,
      memoryLimitMiB: 512,
      environment: {
        LISTINGS: configuredListings,
        MAIN_CLASS: 'group.gnometrading.collectors.DelegatingCollectorOrchestrator',
        OUTPUT_BUCKET: normalizedRawBucket.bucketName,
        POLYMARKET_WS_URL: 'wss://ws-subscriptions-clob.polymarket.com/ws/market',
        REGISTRY_URL: registryUrl.valueAsString,
        STAGE: 'dev',
        VENUE_RAW_BUCKET: venueRawBucket.bucketName,
      },
      secrets: {
        REGISTRY_API_KEY: ecs.Secret.fromSecretsManager(registryApiKey),
      },
      healthCheck: {
        command: ['CMD-SHELL', 'wget --spider --quiet http://localhost:8080/health || exit 1'],
        interval: cdk.Duration.seconds(15),
        timeout: cdk.Duration.seconds(5),
        retries: 3,
        startPeriod: cdk.Duration.seconds(60),
      },
      logging: ecs.LogDrivers.awsLogs({
        logGroup,
        streamPrefix: 'collector',
      }),
      portMappings: [{ containerPort: 8080 }],
      stopTimeout: cdk.Duration.seconds(75),
    });
    const service = new ecs.FargateService(this, 'CollectorService', {
      assignPublicIp: true,
      circuitBreaker: { rollback: true },
      cluster,
      desiredCount: desiredCount.valueAsNumber,
      maxHealthyPercent: 200,
      minHealthyPercent: 100,
      securityGroups: [securityGroup],
      taskDefinition,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
    });

    new cdk.CfnOutput(this, 'CollectorServiceName', { value: service.serviceName });
    new cdk.CfnOutput(this, 'LogGroupName', { value: logGroup.logGroupName });
    new cdk.CfnOutput(this, 'NormalizedRawBucketName', { value: normalizedRawBucket.bucketName });
    new cdk.CfnOutput(this, 'VenueRawBucketName', { value: venueRawBucket.bucketName });
  }
}
