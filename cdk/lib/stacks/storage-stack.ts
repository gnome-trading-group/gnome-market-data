import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3notifications from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import { Construct } from "constructs";
import { MarketDataConfig } from "../config";

export interface StorageStackProps extends cdk.StackProps {
  config: MarketDataConfig;
}

export class StorageStack extends cdk.Stack {
  public readonly collectorsTable: dynamodb.Table;
  public readonly transformJobsTable: dynamodb.Table;
  public readonly gapsTable: dynamodb.Table;
  public readonly rawBucket: s3.Bucket;
  public readonly mergedBucket: s3.Bucket;
  public readonly finalBucket: s3.Bucket;
  public readonly mergerQueue: sqs.Queue;
  public readonly transformerQueue: sqs.Queue;
  public readonly gapQueue: sqs.Queue;

  constructor(scope: Construct, id: string, props: StorageStackProps) {
    super(scope, id, props);

    this.rawBucket = new s3.Bucket(this, 'CollectorRawBucket', {
      bucketName: `gnome-market-data-raw-${props.config.account.stage}`,
    });
    this.mergedBucket = new s3.Bucket(this, 'CollectorArchiveBucket', {
      bucketName: `gnome-market-data-merged-${props.config.account.stage}`,
    });

    this.finalBucket = new s3.Bucket(this, 'CollectorBucket', {
      bucketName: `gnome-market-data-${props.config.account.stage}`,
    });

    this.collectorsTable = new dynamodb.Table(this, "MarketDataCollectorsTable", {
      tableName: "market-data-collectors",
      partitionKey: { name: "listingId", type: dynamodb.AttributeType.NUMBER },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });

    this.transformJobsTable = new dynamodb.Table(this, "MarketDataTransformJobsTable", {
      tableName: "market-data-transform-jobs-old",
      partitionKey: { name: "listingId", type: dynamodb.AttributeType.NUMBER },
      sortKey: { name: "schemaType", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
      timeToLiveAttribute: "expiresAt",
    });

    this.gapsTable = new dynamodb.Table(this, "MarketDataGapsTable", {
      tableName: "market-data-gaps",
      partitionKey: { name: "listingId", type: dynamodb.AttributeType.NUMBER },
      sortKey: { name: "timestamp", type: dynamodb.AttributeType.NUMBER },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecovery: true,
    });

    this.mergerQueue = new sqs.Queue(this, 'MergerQueue', {
      queueName: 'market-data-merger-queue',
      visibilityTimeout: cdk.Duration.minutes(15),
      retentionPeriod: cdk.Duration.hours(4),
      receiveMessageWaitTime: cdk.Duration.seconds(20),
    });

    this.transformerQueue = new sqs.Queue(this, 'TransformerQueue', {
      queueName: 'market-data-transformer-queue',
      visibilityTimeout: cdk.Duration.minutes(15),
      retentionPeriod: cdk.Duration.hours(4),
      receiveMessageWaitTime: cdk.Duration.seconds(20),
    });

    this.gapQueue = new sqs.Queue(this, 'GapQueue', {
      queueName: 'market-data-gap-queue',
      visibilityTimeout: cdk.Duration.minutes(15),
      retentionPeriod: cdk.Duration.hours(4),
      receiveMessageWaitTime: cdk.Duration.seconds(20),
      deliveryDelay: cdk.Duration.minutes(15),
    });

    const rawTopic = new sns.Topic(this, 'MarketDataRawBucketSnsTopic');
    rawTopic.addSubscription(new subs.SqsSubscription(this.mergerQueue));
    this.rawBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3notifications.SnsDestination(rawTopic)
    );

    const mergerTopic = new sns.Topic(this, 'MarketDataMergerBucketSnsTopic');
    mergerTopic.addSubscription(new subs.SqsSubscription(this.transformerQueue));
    mergerTopic.addSubscription(new subs.SqsSubscription(this.gapQueue));
    this.mergedBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3notifications.SnsDestination(mergerTopic)
    );
  }
}