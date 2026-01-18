import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Construct } from 'constructs';
import { JavaLambda } from '../constructs/java-lambda';
import { MarketDataConfig } from '../config';

export interface InventoryProcessorStackProps extends cdk.StackProps {
  metadataBucket: s3.IBucket;
  inventoryQueue: sqs.IQueue;
  coverageTable: dynamodb.ITable;
  config: MarketDataConfig;
}

/**
 * Stack for processing S3 inventory files and populating coverage data in DynamoDB.
 * 
 * Architecture:
 * 1. S3 inventory writes Parquet files to metadata bucket
 * 2. S3 event notification triggers SQS queue
 * 3. Lambda (Java) reads from SQS, processes Parquet, writes to DynamoDB
 */
export class InventoryProcessorStack extends cdk.Stack {
  public readonly processorFunction: lambda.Function;

  constructor(scope: Construct, id: string, props: InventoryProcessorStackProps) {
    super(scope, id, props);

    const processLambda = new JavaLambda(this, 'InventoryProcessorLambda', {
      name: 'InventoryProcessor',
      classPath: 'group.gnometrading.coverage.InventoryProcessorLambdaHandler',
      memorySize: 3008,
      environment: {
        METADATA_BUCKET_NAME: props.metadataBucket.bucketName,
        COVERAGE_TABLE_NAME: props.coverageTable.tableName,
        STAGE: props.config.account.stage,
      },
    });
    this.processorFunction = processLambda.lambdaFunction;

    props.metadataBucket.grantRead(this.processorFunction);
    props.coverageTable.grantReadWriteData(this.processorFunction);

    this.processorFunction.addEventSource(new SqsEventSource(props.inventoryQueue, {
      batchSize: 1,
      maxBatchingWindow: cdk.Duration.seconds(0),
    }));
  }
}

