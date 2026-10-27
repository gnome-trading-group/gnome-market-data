import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";
import { JavaLambda } from "../constructs/java-lambda";
import { LAMBDAS_VERSION, MarketDataConfig } from "../config";

export interface QualityCheckStackProps extends cdk.StackProps {
  finalBucket: s3.IBucket;
  qualityIssuesTable: dynamodb.ITable;
  dailyListingStatisticsTable: dynamodb.ITable;
  config: MarketDataConfig;
}

export class QualityCheckStack extends cdk.Stack {
  public readonly qualityCheckLambda: lambda.Function;
  public readonly qualityBackfillLambda: lambda.Function;
  public readonly qualityInvestigationLambda: lambda.Function;

  constructor(scope: Construct, id: string, props: QualityCheckStackProps) {
    super(scope, id, props);

    const registryKeyArn = cdk.Fn.importValue('RegistryApiKeyArn');
    const registryKeyPolicy = new iam.PolicyStatement({
      actions: ['apigateway:GET'],
      resources: [registryKeyArn],
    });

    const sharedEnv = {
      FINAL_BUCKET_NAME: props.finalBucket.bucketName,
      QUALITY_ISSUES_TABLE_NAME: props.qualityIssuesTable.tableName,
      LISTING_STATISTICS_TABLE_NAME: props.dailyListingStatisticsTable.tableName,
      STAGE: props.config.account.stage,
      REGISTRY_API_KEY_ID: cdk.Fn.importValue('RegistryApiKeyId'),
    };

    const qualityCheckLambda = new JavaLambda(this, `QualityCheckLambda-${LAMBDAS_VERSION}`, {
      name: `QualityCheck-${LAMBDAS_VERSION}`,
      classPath: 'group.gnometrading.quality.QualityCheckLambdaHandler',
      environment: {
        ...sharedEnv,
        // Read for market-data's quality_checks.enabled switch.
        CONTROLLER_API_URL: cdk.Fn.importValue('ControllerApiUrl'),
        CONTROLLER_API_KEY_ID: cdk.Fn.importValue('ControllerServiceConfigApiKeyId'),
      },
    });
    this.qualityCheckLambda = qualityCheckLambda.lambdaFunction;
    this.qualityCheckLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['apigateway:GET'],
      resources: [`arn:aws:apigateway:${this.region}::/apikeys/${cdk.Fn.importValue('ControllerServiceConfigApiKeyId')}`],
    }));

    props.finalBucket.grantRead(this.qualityCheckLambda);
    props.qualityIssuesTable.grantReadWriteData(this.qualityCheckLambda);
    props.dailyListingStatisticsTable.grantReadWriteData(this.qualityCheckLambda);
    this.qualityCheckLambda.addToRolePolicy(registryKeyPolicy);

    // No event source: quality checks are off (see StorageStack). The handler also honours market-data's
    // quality_checks.enabled once re-wired.
    const qualityBackfillLambda = new JavaLambda(this, `QualityBackfillLambda-${LAMBDAS_VERSION}`, {
      name: `QualityBackfill-${LAMBDAS_VERSION}`,
      classPath: 'group.gnometrading.quality.QualityBackfillLambdaHandler',
      environment: sharedEnv,
    });
    this.qualityBackfillLambda = qualityBackfillLambda.lambdaFunction;

    props.finalBucket.grantRead(this.qualityBackfillLambda);
    props.qualityIssuesTable.grantReadWriteData(this.qualityBackfillLambda);
    props.dailyListingStatisticsTable.grantReadWriteData(this.qualityBackfillLambda);
    this.qualityBackfillLambda.addToRolePolicy(registryKeyPolicy);

    const qualityInvestigationLambda = new JavaLambda(this, `QualityInvestigationLambda-${LAMBDAS_VERSION}`, {
      name: `QualityInvestigation-${LAMBDAS_VERSION}`,
      classPath: 'group.gnometrading.quality.MinuteInvestigationLambdaHandler',
      environment: sharedEnv,
    });
    this.qualityInvestigationLambda = qualityInvestigationLambda.lambdaFunction;

    props.finalBucket.grantRead(this.qualityInvestigationLambda);
    props.qualityIssuesTable.grantReadData(this.qualityInvestigationLambda);
    props.dailyListingStatisticsTable.grantReadData(this.qualityInvestigationLambda);
    this.qualityInvestigationLambda.addToRolePolicy(registryKeyPolicy);

    // Retired with its handler; the backend stack that deploys after this one still imports it, so it stays one
    // release, invoked by nothing. The next release removes it.
    const retiredBboTimeline = new JavaLambda(this, `BboTimelineLambda-${LAMBDAS_VERSION}`, {
      name: `BboTimeline-${LAMBDAS_VERSION}`,
      classPath: 'group.gnometrading.bbo.BboTimelineLambdaHandler',
    }).lambdaFunction;
    this.exportValue(retiredBboTimeline.functionArn);
    this.exportValue(retiredBboTimeline.functionName);
  }
}
