import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3notifications from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";
import { MarketDataConfig } from "../config";

export interface StorageStackProps extends cdk.StackProps {
  config: MarketDataConfig;
}

export interface MonitoredQueue {
  name: string;
  queue: sqs.IQueue;
  deadLetterQueue: sqs.IQueue;
  maxAge: cdk.Duration;
}

// A message that fails three receives (one bad key, not the whole batch: the handlers report item failures) is set
// aside for two weeks instead of retrying until it expires.
const MAX_RECEIVES = 3;
const DEAD_LETTER_RETENTION = cdk.Duration.days(14);

export class StorageStack extends cdk.Stack {
  public readonly gapsTable: dynamodb.Table;
  public readonly coverageTable: dynamodb.Table;
  // The collection control plane (Phase 6): what to collect, where it runs, and what was collected when.
  public readonly collectionRulesTable: dynamodb.Table;
  public readonly collectionTargetsTable: dynamodb.Table;
  public readonly collectionShardsTable: dynamodb.Table;
  public readonly collectionCoverageTable: dynamodb.Table;
  public readonly collectionGapsTable: dynamodb.Table;
  public readonly finalBucket: s3.Bucket;
  public readonly metadataBucket: s3.Bucket;
  public readonly exchangeRawBucket: s3.Bucket;
  public readonly inventoryQueue: sqs.Queue;
  public readonly qualityIssuesTable: dynamodb.Table;
  public readonly dailyListingStatisticsTable: dynamodb.Table;
  public readonly qualityCheckQueue: sqs.Queue;
  // Each pipeline queue with its dead-letter queue and how old its oldest message may get, for the monitoring stack.
  public readonly monitoredQueues: MonitoredQueue[] = [];

  constructor(scope: Construct, id: string, props: StorageStackProps) {
    super(scope, id, props);

    // Shards write each minute here directly, once (gnome-strategies CycleManager, conditional put).
    this.finalBucket = new s3.Bucket(this, 'CollectorBucket', {
      bucketName: `gnome-market-data-${props.config.account.stage}`,
    });
    this.metadataBucket = new s3.Bucket(this, 'CollectorMetadataBucket', {
      bucketName: `gnome-market-data-metadata-${props.config.account.stage}`,
      // Shard statuses (collection/shards/<shard>/status/<task>.json) are rewritten every 15 s while a task lives; a
      // dead task's last one only matters until the reconciler has closed its coverage.
      lifecycleRules: [{ prefix: 'collection/', expiration: cdk.Duration.days(1) }],
    });
    this.exchangeRawBucket = new s3.Bucket(this, 'ExchangeRawBucket', {
      bucketName: `gnome-market-data-exchange-raw-${props.config.account.stage}`,
      lifecycleRules: [{
        expiration: cdk.Duration.days(30),
      }],
    });

    this.finalBucket.addInventory({
      inventoryId: 'market-data-inventory',
      format: s3.InventoryFormat.CSV,
      frequency: s3.InventoryFrequency.DAILY,
      optionalFields: [
        "Size",
      ],
      destination: {
        bucket: this.metadataBucket,
        prefix: 'market-data-inventory',
      },
    });

    this.gapsTable = new dynamodb.Table(this, "MarketDataGapsTable", {
      tableName: "market-data-gaps",
      partitionKey: { name: "listingId", type: dynamodb.AttributeType.NUMBER },
      sortKey: { name: "timestamp", type: dynamodb.AttributeType.NUMBER },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });

    this.gapsTable.addGlobalSecondaryIndex({
      indexName: "status-timestamp-index",
      partitionKey: { name: "status", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "timestamp", type: dynamodb.AttributeType.NUMBER },
    });

    this.coverageTable = new dynamodb.Table(this, "CoverageTable", {
      tableName: "market-data-coverage",
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });

    this.collectionRulesTable = this.collectionTable("CollectionRulesTable", "collection-rules", {
      name: "rule_id", type: dynamodb.AttributeType.STRING,
    });

    // One row per stream wanted, or retiring: which shard it is on, and why it is wanted.
    this.collectionTargetsTable = this.collectionTable("CollectionTargetsTable", "collection-targets", {
      name: "stream_id", type: dynamodb.AttributeType.NUMBER,
    });
    this.collectionTargetsTable.addGlobalSecondaryIndex({
      indexName: "shard_id-index",
      partitionKey: { name: "shard_id", type: dynamodb.AttributeType.STRING },
    });

    // A shard's item is its assignment: shards in every region read it (gnome-orchestrator DynamoAssignment).
    this.collectionShardsTable = this.collectionTable("CollectionShardsTable", "collection-shards", {
      name: "shard_id", type: dynamodb.AttributeType.STRING,
    });

    // Intervals each stream was live and verified on a task; open ones carry open_shard_id, so a shard's are one query.
    this.collectionCoverageTable = this.collectionTable(
      "CollectionCoverageTable",
      "collection-coverage",
      { name: "stream_id", type: dynamodb.AttributeType.NUMBER },
      { name: "from_ns", type: dynamodb.AttributeType.NUMBER },
    );
    this.collectionCoverageTable.addGlobalSecondaryIndex({
      indexName: "open_shard_id-index",
      partitionKey: { name: "open_shard_id", type: dynamodb.AttributeType.STRING },
    });

    // Time a stream was wanted but not covered, with why.
    this.collectionGapsTable = this.collectionTable(
      "CollectionGapsTable",
      "collection-gaps",
      { name: "stream_id", type: dynamodb.AttributeType.NUMBER },
      { name: "from_ns", type: dynamodb.AttributeType.NUMBER },
    );

    this.qualityIssuesTable = new dynamodb.Table(this, "QualityIssuesTable", {
      tableName: "market-data-quality-issues",
      partitionKey: { name: "listingId", type: dynamodb.AttributeType.NUMBER },
      sortKey: { name: "issueId", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });

    this.qualityIssuesTable.addGlobalSecondaryIndex({
      indexName: "status-timestamp-index",
      partitionKey: { name: "status", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "timestamp", type: dynamodb.AttributeType.NUMBER },
    });

    this.qualityIssuesTable.addGlobalSecondaryIndex({
      indexName: "ruleType-timestamp-index",
      partitionKey: { name: "ruleType", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "timestamp", type: dynamodb.AttributeType.NUMBER },
    });

    this.dailyListingStatisticsTable = new dynamodb.Table(this, "DailyListingStatisticsTable", {
      tableName: "market-data-listing-daily-statistics",
      partitionKey: { name: "listingId", type: dynamodb.AttributeType.NUMBER },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });

    this.qualityCheckQueue = this.monitoredQueue('QualityCheckQueue', 'market-data-quality-check-queue', cdk.Duration.minutes(15), {
      visibilityTimeout: cdk.Duration.minutes(15),
      retentionPeriod: cdk.Duration.days(4),
      receiveMessageWaitTime: cdk.Duration.seconds(20),
    });

    this.inventoryQueue = this.monitoredQueue('InventoryQueue', 'market-data-inventory-queue', cdk.Duration.minutes(15), {
      visibilityTimeout: cdk.Duration.minutes(15),
      retentionPeriod: cdk.Duration.days(4),
      receiveMessageWaitTime: cdk.Duration.seconds(20),
    });

    // Quality checks are off (2026-10-27: not worth their cost yet). Re-enable by sending the final bucket's new objects
    // to this.qualityCheckQueue and giving QualityCheckStack's Lambda its SQS event source back.

    this.retireOldPipeline(props);

    this.metadataBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3notifications.SqsDestination(this.inventoryQueue),
      { prefix: 'market-data-inventory/', suffix: '.csv.gz' }
    );
  }

  /**
   * The pipeline before Phase 6 (merger, transformer, gap detector, per-listing collectors, raw and merged buckets),
   * kept unused for one release: the stacks that imported these deploy after this one, and an export still imported
   * can't be deleted. The next release removes all of it; the raw and merged buckets are retained (empty and delete
   * them by hand).
   */
  private retireOldPipeline(props: StorageStackProps) {
    const rawBucket = new s3.Bucket(this, 'CollectorRawBucket', {
      bucketName: `gnome-market-data-raw-${props.config.account.stage}`,
    });
    const mergedBucket = new s3.Bucket(this, 'CollectorArchiveBucket', {
      bucketName: `gnome-market-data-merged-${props.config.account.stage}`,
    });
    const collectorsTable = new dynamodb.Table(this, "MarketDataCollectorsTable", {
      tableName: "market-data-collectors",
      partitionKey: { name: "listingId", type: dynamodb.AttributeType.NUMBER },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });

    const transformJobsTable = new dynamodb.Table(this, "MarketDataTransformJobsTable", {
      tableName: "market-data-transform-jobs",
      partitionKey: { name: "jobId", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "timestamp", type: dynamodb.AttributeType.NUMBER },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
      timeToLiveAttribute: "expiresAt",
    });

    // Used for the lambda that process jobs
    transformJobsTable.addGlobalSecondaryIndex({
      indexName: "schemaType-status-index",
      partitionKey: { name: "schemaType", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "status", type: dynamodb.AttributeType.STRING },
    });

    // Used for /transform-jobs/search
    transformJobsTable.addGlobalSecondaryIndex({
      indexName: "listingId-schemaType-index",
      partitionKey: { name: "listingId", type: dynamodb.AttributeType.NUMBER },
      sortKey: { name: "schemaType", type: dynamodb.AttributeType.STRING },
    });

    // Used for /transform-jobs/list
    transformJobsTable.addGlobalSecondaryIndex({
      indexName: "status-timestamp-index",
      partitionKey: { name: "status", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "timestamp", type: dynamodb.AttributeType.NUMBER },
    });

    const mergerQueue = this.retiredQueue('MergerQueue', 'market-data-merger-queue', cdk.Duration.minutes(15), {
      visibilityTimeout: cdk.Duration.minutes(15),
      retentionPeriod: cdk.Duration.days(4),
      receiveMessageWaitTime: cdk.Duration.seconds(20),
    });

    const transformerQueue = this.retiredQueue('TransformerQueue', 'market-data-transformer-queue', cdk.Duration.minutes(15), {
      visibilityTimeout: cdk.Duration.minutes(15),
      retentionPeriod: cdk.Duration.days(4),
      receiveMessageWaitTime: cdk.Duration.seconds(20),
    });

    const gapQueue = this.retiredQueue('GapQueue', 'market-data-gap-queue', cdk.Duration.minutes(30), {
      visibilityTimeout: cdk.Duration.minutes(15),
      retentionPeriod: cdk.Duration.days(4),
      receiveMessageWaitTime: cdk.Duration.seconds(20),
      deliveryDelay: cdk.Duration.minutes(15),
    });

    for (const bucket of [rawBucket, mergedBucket]) {
      this.exportValue(bucket.bucketName);
      this.exportValue(bucket.bucketArn);
    }
    for (const table of [collectorsTable, transformJobsTable]) {
      this.exportValue(table.tableName);
      this.exportValue(table.tableArn);
    }
    for (const queue of [mergerQueue, transformerQueue, gapQueue]) {
      this.exportValue(queue.queueArn);
      this.exportValue(queue.queueName);
      this.exportValue(queue.deadLetterQueue!.queue.queueName);
    }
    // Still imported by stacks that deploy after this one or are deleted by hand: the quality queue by the quality
    // check stack (which no longer reads it), the exchange-raw bucket by the old us-east-1 collector stack.
    this.exportValue(this.qualityCheckQueue.queueArn);
    this.exportValue(this.exchangeRawBucket.bucketName);
  }

  /** A queue as monitoredQueue builds it (same ids, so the same resources), without its alarms. */
  private retiredQueue(id: string, name: string, _maxAge: cdk.Duration, props: sqs.QueueProps): sqs.Queue {
    const deadLetterQueue = new sqs.Queue(this, `${id}DeadLetter`, {
      queueName: `${name}-dlq`,
      retentionPeriod: DEAD_LETTER_RETENTION,
    });
    return new sqs.Queue(this, id, {
      ...props,
      queueName: name,
      deadLetterQueue: { queue: deadLetterQueue, maxReceiveCount: MAX_RECEIVES },
    });
  }

  private collectionTable(
    id: string,
    tableName: string,
    partitionKey: dynamodb.Attribute,
    sortKey?: dynamodb.Attribute,
  ): dynamodb.Table {
    return new dynamodb.Table(this, id, {
      tableName,
      partitionKey,
      sortKey,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });
  }

  private monitoredQueue(id: string, name: string, maxAge: cdk.Duration, props: sqs.QueueProps): sqs.Queue {
    const deadLetterQueue = new sqs.Queue(this, `${id}DeadLetter`, {
      queueName: `${name}-dlq`,
      retentionPeriod: DEAD_LETTER_RETENTION,
    });
    const queue = new sqs.Queue(this, id, {
      ...props,
      queueName: name,
      deadLetterQueue: { queue: deadLetterQueue, maxReceiveCount: MAX_RECEIVES },
    });
    this.monitoredQueues.push({ name, queue, deadLetterQueue, maxAge });
    return queue;
  }
}
