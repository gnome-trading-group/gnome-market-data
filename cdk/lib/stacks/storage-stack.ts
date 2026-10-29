import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";
import { MarketDataConfig } from "../config";

export interface StorageStackProps extends cdk.StackProps {
  config: MarketDataConfig;
}

export class StorageStack extends cdk.Stack {
  // The collection control plane (Phase 6): what to collect, where it runs, and what was collected when.
  public readonly collectionRulesTable: dynamodb.Table;
  public readonly collectionTargetsTable: dynamodb.Table;
  public readonly collectionShardsTable: dynamodb.Table;
  public readonly collectionCoverageTable: dynamodb.Table;
  public readonly collectionGapsTable: dynamodb.Table;
  public readonly collectionLeasesTable: dynamodb.Table;
  public readonly finalBucket: s3.Bucket;
  public readonly metadataBucket: s3.Bucket;
  public readonly exchangeRawBucket: s3.Bucket;

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

    // Sessions' leases on streams (lease_id = session id), written by the lease API and released by it or the
    // reconciler. A released lease carries `ttl` and goes a week later; `expires_at` is the lease's own expiry, not this.
    this.collectionLeasesTable = new dynamodb.Table(this, "CollectionLeasesTable", {
      tableName: "collection-leases",
      partitionKey: { name: "lease_id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
      timeToLiveAttribute: "ttl",
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

}
