import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";
import { JavaLambda } from "../constructs/java-lambda";
import { MarketDataConfig } from "../config";

export interface MergerStackProps extends cdk.StackProps {
  rawBucket: s3.Bucket;
  mergedBucket: s3.Bucket;
  mergerQueue: sqs.IQueue;
  config: MarketDataConfig;
}

export class MergerStack extends cdk.Stack {
  public readonly mergerLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: MergerStackProps) {
    super(scope, id, props);

    const javaLambda = new JavaLambda(this, 'MergerLambda', {
      name: 'Merger',
      classPath: 'group.gnometrading.merger.MergerLambdaHandler',
      environment: {
        RAW_BUCKET_NAME: props.rawBucket.bucketName,
        MERGED_BUCKET_NAME: props.mergedBucket.bucketName,
        STAGE: props.config.account.stage,
      },
    });
    this.mergerLambda = javaLambda.lambdaFunction;

    props.rawBucket.grantRead(this.mergerLambda);
    props.mergedBucket.grantReadWrite(this.mergerLambda);

    this.mergerLambda.addEventSource(new lambdaEventSources.SqsEventSource(props.mergerQueue, {
      batchSize: 1_000,
      maxBatchingWindow: cdk.Duration.minutes(3),
    }));
  }
}
