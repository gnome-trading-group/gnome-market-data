import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";
import { JavaLambda } from "../constructs/java-lambda";
import { LAMBDAS_VERSION, MarketDataConfig } from "../config";

export interface QualityCheckStackProps extends cdk.StackProps {
  mergedBucket: s3.Bucket;
  qualityIssuesTable: dynamodb.ITable;
  dailyListingStatisticsTable: dynamodb.ITable;
  qualityCheckQueue: sqs.IQueue;
  config: MarketDataConfig;
}

export class QualityCheckStack extends cdk.Stack {
  public readonly qualityCheckLambda: lambda.Function;
  public readonly qualityBackfillLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: QualityCheckStackProps) {
    super(scope, id, props);

    const sharedEnv = {
      MERGED_BUCKET_NAME: props.mergedBucket.bucketName,
      QUALITY_ISSUES_TABLE_NAME: props.qualityIssuesTable.tableName,
      LISTING_STATISTICS_TABLE_NAME: props.dailyListingStatisticsTable.tableName,
      STAGE: props.config.account.stage,
    };

    const qualityCheckLambda = new JavaLambda(this, `QualityCheckLambda-${LAMBDAS_VERSION}`, {
      name: `QualityCheck-${LAMBDAS_VERSION}`,
      classPath: 'group.gnometrading.quality.QualityCheckLambdaHandler',
      environment: sharedEnv,
    });
    this.qualityCheckLambda = qualityCheckLambda.lambdaFunction;

    props.mergedBucket.grantRead(this.qualityCheckLambda);
    props.qualityIssuesTable.grantReadWriteData(this.qualityCheckLambda);
    props.dailyListingStatisticsTable.grantReadWriteData(this.qualityCheckLambda);

    this.qualityCheckLambda.addEventSource(new lambdaEventSources.SqsEventSource(props.qualityCheckQueue, {
      batchSize: 1_000,
      maxBatchingWindow: cdk.Duration.minutes(3),
    }));

    const qualityBackfillLambda = new JavaLambda(this, `QualityBackfillLambda-${LAMBDAS_VERSION}`, {
      name: `QualityBackfill-${LAMBDAS_VERSION}`,
      classPath: 'group.gnometrading.quality.QualityBackfillLambdaHandler',
      environment: sharedEnv,
    });
    this.qualityBackfillLambda = qualityBackfillLambda.lambdaFunction;

    props.mergedBucket.grantRead(this.qualityBackfillLambda);
    props.qualityIssuesTable.grantReadWriteData(this.qualityBackfillLambda);
    props.dailyListingStatisticsTable.grantReadWriteData(this.qualityBackfillLambda);
  }
}
