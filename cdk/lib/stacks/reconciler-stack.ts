import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";
import { JavaLambda } from "../constructs/java-lambda";
import { LAMBDAS_VERSION, MarketDataConfig } from "../config";
import { collectionNames } from "./collection-regional-stack";

export interface ReconcilerStackProps extends cdk.StackProps {
  config: MarketDataConfig;
  regions: string[];
  rulesTable: dynamodb.ITable;
  targetsTable: dynamodb.ITable;
  shardsTable: dynamodb.ITable;
  coverageTable: dynamodb.ITable;
  gapsTable: dynamodb.ITable;
  leasesTable: dynamodb.ITable;
  metadataBucket: s3.IBucket;
}

/**
 * The collection reconciler: the only writer of targets and shards, run one tick at a time. Its FIFO queue has one
 * message group and the Lambda one concurrent run, so ticks never overlap and a burst of wake-ups (rule changes,
 * triggers) costs one tick; a schedule enqueues a tick every minute as the resync.
 */
export class ReconcilerStack extends cdk.Stack {
  public readonly reconcilerLambda: lambda.Function;
  public readonly wakeQueue: sqs.Queue;

  constructor(scope: Construct, id: string, props: ReconcilerStackProps) {
    super(scope, id, props);

    // Deduplicated by content: each scheduled tick carries its minute, and the API's wake-ups set their own id.
    this.wakeQueue = new sqs.Queue(this, "ReconcilerQueue", {
      queueName: "collection-reconciler.fifo",
      fifo: true,
      contentBasedDeduplication: true,
      // At least the Lambda's timeout (SQS triggers require it), and no more: a failed tick holds the one message group
      // until its batch is visible again.
      visibilityTimeout: cdk.Duration.minutes(3),
      retentionPeriod: cdk.Duration.hours(1),
    });

    const reconciler = new JavaLambda(this, `ReconcilerLambda-${LAMBDAS_VERSION}`, {
      name: `Reconciler-${LAMBDAS_VERSION}`,
      classPath: "group.gnometrading.collection.ReconcilerLambdaHandler",
      environment: {
        STAGE: props.config.account.stage,
        REGISTRY_API_KEY_ID: cdk.Fn.importValue("RegistryApiKeyId"),
        COLLECTION_RULES_TABLE: props.rulesTable.tableName,
        COLLECTION_TARGETS_TABLE: props.targetsTable.tableName,
        COLLECTION_SHARDS_TABLE: props.shardsTable.tableName,
        COLLECTION_COVERAGE_TABLE: props.coverageTable.tableName,
        COLLECTION_GAPS_TABLE: props.gapsTable.tableName,
        COLLECTION_LEASES_TABLE: props.leasesTable.tableName,
        METADATA_BUCKET_NAME: props.metadataBucket.bucketName,
        CONTROLLER_API_URL: cdk.Fn.importValue("ControllerApiUrl"),
        CONTROLLER_API_KEY_ID: cdk.Fn.importValue("ControllerServiceConfigApiKeyId"),
      },
      // One tick at a time: every tick is a full read-modify-write of targets and shards.
      reservedConcurrentExecutions: 1,
      // A tick used 376 MB at 50 streams; 1 GB leaves room as targets grow at half the cost of the 2 GB default.
      memorySize: 1024,
      timeout: cdk.Duration.minutes(2),
    });
    this.reconcilerLambda = reconciler.lambdaFunction;
    const fn = this.reconcilerLambda;
    fn.addEventSource(new lambdaEventSources.SqsEventSource(this.wakeQueue, { batchSize: 10 }));

    new events.Rule(this, "ReconcilerTick", {
      schedule: events.Schedule.rate(cdk.Duration.minutes(1)),
      targets: [new targets.SqsQueue(this.wakeQueue, {
        messageGroupId: "reconcile",
        message: events.RuleTargetInput.fromText(`tick:${events.EventField.time}`),
      })],
    });

    for (const table of [props.rulesTable, props.targetsTable, props.shardsTable, props.coverageTable, props.gapsTable,
      props.leasesTable]) {
      table.grantReadWriteData(fn);
    }
    fn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["s3:ListBucket"],
      resources: [props.metadataBucket.bucketArn],
      conditions: { StringLike: { "s3:prefix": ["collection/*"] } },
    }));
    fn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["s3:GetObject"],
      resources: [props.metadataBucket.arnForObjects("collection/*")],
    }));

    const ecsResources: string[] = [];
    const passRoles: string[] = [];
    for (const region of props.regions) {
      const names = collectionNames(region);
      ecsResources.push(
        `arn:aws:ecs:${region}:${this.account}:service/${names.cluster}/*`,
        `arn:aws:ecs:${region}:${this.account}:task-definition/*`,
      );
      passRoles.push(
        `arn:aws:iam::${this.account}:role/${names.taskRole}`,
        `arn:aws:iam::${this.account}:role/${names.executionRole}`,
      );
    }
    fn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["ecs:CreateService", "ecs:DeleteService", "ecs:DescribeServices", "ecs:TagResource"],
      resources: ecsResources,
    }));
    // Task definitions are described, registered, listed and deregistered by family, which IAM can't scope by resource.
    fn.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "ecs:DescribeTaskDefinition",
        "ecs:RegisterTaskDefinition",
        "ecs:ListTaskDefinitions",
        "ecs:DeregisterTaskDefinition",
      ],
      resources: ["*"],
    }));
    fn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["iam:PassRole"],
      resources: passRoles,
      conditions: { StringEquals: { "iam:PassedToService": "ecs-tasks.amazonaws.com" } },
    }));
    fn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["events:PutEvents"],
      resources: [`arn:aws:events:${this.region}:${this.account}:event-bus/gnome-alerts`],
    }));
    fn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["ssm:GetParameter"],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter/gnome/orchestrator/latest-version`],
    }));
    fn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["apigateway:GET"],
      resources: [
        cdk.Fn.importValue("RegistryApiKeyArn"),
        `arn:aws:apigateway:${this.region}::/apikeys/${cdk.Fn.importValue("ControllerServiceConfigApiKeyId")}`,
      ],
    }));
  }
}
