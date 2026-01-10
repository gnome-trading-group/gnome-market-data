import * as cdk from "aws-cdk-lib";
// import * as events from "aws-cdk-lib/aws-events";
// import * as targets from "aws-cdk-lib/aws-events-targets";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import { Construct } from "constructs";
import { JavaLambda } from "../constructs/java-lambda";
import { SchemaType } from "@gnome-trading-group/gnome-shared-cdk";
import { MarketDataConfig } from "../config";

export interface TransformerStackProps extends cdk.StackProps {
  mergedBucket: s3.Bucket;
  finalBucket: s3.Bucket;
  transformJobsTable: dynamodb.ITable;
  transformerQueue: sqs.IQueue;
  config: MarketDataConfig;
}

export class TransformerStack extends cdk.Stack {
  public readonly transformerJobCreatorLambda: lambda.Function;
  public readonly transformerJobProcessorLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: TransformerStackProps) {
    super(scope, id, props);

    const transformerJobCreatorLambda = new JavaLambda(this, 'TransformerJobCreatorLambda', {
      classPath: 'group.gnometrading.transformer.JobCreatorLambdaHandler',
      environment: {
        MERGED_BUCKET_NAME: props.mergedBucket.bucketName,
        TRANSFORM_JOBS_TABLE_NAME: props.transformJobsTable.tableName,
        STAGE: props.config.account.stage,
      },
    });
    this.transformerJobCreatorLambda = transformerJobCreatorLambda.lambdaFunction;

    props.mergedBucket.grantRead(this.transformerJobCreatorLambda);
    props.transformJobsTable.grantReadWriteData(this.transformerJobCreatorLambda);

    this.transformerJobCreatorLambda.addEventSource(new lambdaEventSources.SqsEventSource(props.transformerQueue, {
      batchSize: 1_000,
      maxBatchingWindow: cdk.Duration.minutes(3),
    }));

    const transformerJobProcessorLambda = new JavaLambda(this, 'TransformerJobProcessorLambda', {
      classPath: 'group.gnometrading.transformer.JobProcessorLambdaHandler',
      environment: {
        MERGED_BUCKET_NAME: props.mergedBucket.bucketName,
        FINAL_BUCKET_NAME: props.finalBucket.bucketName,
        TRANSFORM_JOBS_TABLE_NAME: props.transformJobsTable.tableName,
        STAGE: props.config.account.stage,
      },
    });
    this.transformerJobProcessorLambda = transformerJobProcessorLambda.lambdaFunction;

    props.mergedBucket.grantRead(this.transformerJobProcessorLambda);
    props.finalBucket.grantReadWrite(this.transformerJobProcessorLambda);
    props.transformJobsTable.grantReadWriteData(this.transformerJobProcessorLambda);

    // TODO: Uncomment when ready to enable scheduled processing
    // for (const schemaType of Object.values(SchemaType)) {
    //   const schemaDuration = this.getSchemaDuration(schemaType);
    //   const lambdaRule = new events.Rule(this, `TransformerJobProcessorRule-${schemaType}`, {
    //     schedule: events.Schedule.rate(schemaDuration),
    //   });
    //   lambdaRule.addTarget(new targets.LambdaFunction(this.transformerJobProcessorLambda, {
    //     event: events.RuleTargetInput.fromObject({
    //       schemaType,
    //     }),
    //   }));
    // }
  }

  private getSchemaDuration(schemaType: SchemaType): cdk.Duration {
    switch (schemaType) {
      case SchemaType.MBO:
      case SchemaType.MBP_10:
      case SchemaType.MBP_1:
      case SchemaType.BBO_1S:
      case SchemaType.BBO_1M:
      case SchemaType.TRADES:
      case SchemaType.OHLCV_1S:
      case SchemaType.OHLCV_1M:
        return cdk.Duration.minutes(15);
      case SchemaType.OHLCV_1H:
        return cdk.Duration.hours(2);
      default:
        throw new Error(`Unknown schema type: ${schemaType}`);
    }
  }
}
