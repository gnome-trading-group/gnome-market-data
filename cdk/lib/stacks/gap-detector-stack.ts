import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";
import { JavaLambda } from "../constructs/java-lambda";
import { LAMBDAS_VERSION, MarketDataConfig } from "../config";

export interface GapDetectorStackProps extends cdk.StackProps {
  mergedBucket: s3.Bucket;
  gapsTable: dynamodb.ITable;
  transformJobsTable: dynamodb.ITable;
  dailyListingStatisticsTable: dynamodb.ITable;
  gapQueue: sqs.IQueue;
  config: MarketDataConfig;
}

export class GapDetectorStack extends cdk.Stack {
  public readonly gapLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: GapDetectorStackProps) {
    super(scope, id, props);

    const gapLambda = new JavaLambda(this, `GapLambda-${LAMBDAS_VERSION}`, {
      name: `GapDetector-${LAMBDAS_VERSION}`,
      classPath: 'group.gnometrading.gap.GapLambdaHandler',
      environment: {
        MERGED_BUCKET_NAME: props.mergedBucket.bucketName,
        GAPS_TABLE_NAME: props.gapsTable.tableName,
        TRANSFORM_JOBS_TABLE_NAME: props.transformJobsTable.tableName,
        LISTING_STATISTICS_TABLE_NAME: props.dailyListingStatisticsTable.tableName,
        STAGE: props.config.account.stage,
        REGISTRY_API_KEY_ID: cdk.Fn.importValue('RegistryApiKeyId'),
      },
    });
    this.gapLambda = gapLambda.lambdaFunction;

    props.mergedBucket.grantRead(this.gapLambda);
    props.gapsTable.grantReadWriteData(this.gapLambda);
    props.transformJobsTable.grantReadData(this.gapLambda);
    props.dailyListingStatisticsTable.grantReadData(this.gapLambda);
    this.gapLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['apigateway:GET'],
      resources: [cdk.Fn.importValue('RegistryApiKeyArn')],
    }));

    this.gapLambda.addEventSource(new lambdaEventSources.SqsEventSource(props.gapQueue, {
      batchSize: 1_000,
      maxBatchingWindow: cdk.Duration.minutes(3),
    }));
  }
}
