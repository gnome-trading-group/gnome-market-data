import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";
import { JavaLambda } from "../constructs/java-lambda";

export interface GapDetectorStackProps extends cdk.StackProps {
  mergedBucket: s3.Bucket;
  gapsTable: dynamodb.ITable;
  gapQueue: sqs.IQueue;
}

export class GapDetectorStack extends cdk.Stack {
  public readonly gapLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: GapDetectorStackProps) {
    super(scope, id, props);

    const gapLambda = new JavaLambda(this, 'GapLambda', {
      classPath: 'group.gnometrading.gap.GapLambdaHandler',
      environment: {
        MERGED_BUCKET_NAME: props.mergedBucket.bucketName,
        GAPS_TABLE_NAME: props.gapsTable.tableName,
      },
    });
    this.gapLambda = gapLambda.lambdaFunction;

    props.mergedBucket.grantRead(this.gapLambda);
    props.gapsTable.grantReadWriteData(this.gapLambda);

    this.gapLambda.addEventSource(new lambdaEventSources.SqsEventSource(props.gapQueue, {
      batchSize: 10,
      maxBatchingWindow: cdk.Duration.seconds(5),
    }));
  }
}
