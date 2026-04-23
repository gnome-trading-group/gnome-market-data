import * as cdk from "aws-cdk-lib";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Construct } from "constructs";

export interface CollectorRegionConfig {
  region: string;
  clusterName: string;
  clusterArn: string;
  taskDefinitionFamily: string;
  securityGroupId: string;
  subnetIds: string[];
  logGroupName: string;
}

interface BackendStackProps extends cdk.StackProps {
  collectorsTable: dynamodb.ITable;
  collectorRegions: Record<string, CollectorRegionConfig>;
  collectorDeploymentVersion: string;
  collectorEventBus: events.IEventBus;
  transformJobsTable: dynamodb.ITable;
  gapsTable: dynamodb.ITable;
  finalBucket: s3.IBucket;
  metadataBucket: s3.IBucket;
  coverageTable: dynamodb.ITable;
  qualityIssuesTable: dynamodb.ITable;
  dailyListingStatisticsTable: dynamodb.ITable;
  qualityBackfillLambda: lambda.IFunction;
  qualityInvestigationLambda: lambda.IFunction;
}

interface EndpointConfig {
  name: string;
  path: string;
  method: string;
  handlerPath: string;
}

export class BackendStack extends cdk.Stack {

  public readonly api: apigateway.RestApi;

  constructor(scope: Construct, id: string, props: BackendStackProps) {
    super(scope, id, props);

    this.api = new apigateway.RestApi(this, "MarketDataApi", {
      restApiName: 'market-data-api',
      description: "API for market data backend services",
      deployOptions: {
        stageName: 'api',
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: apigateway.Cors.ALL_METHODS,
        allowHeaders: [
          ...apigateway.Cors.DEFAULT_HEADERS,
          'Authorization',
          'Content-Type',
          'X-Amz-Date',
          'X-Api-Key',
          'X-Amz-Security-Token'
        ],
      },
    });

    const userPool = cognito.UserPool.fromUserPoolId(this, "UserPool", cdk.Fn.importValue('UserPoolId'));

    const authorizer = new apigateway.CognitoUserPoolsAuthorizer(this, "CognitoAuthorizer", {
      cognitoUserPools: [userPool],
      identitySource: 'method.request.header.Authorization',
    });

    // AWS managed layer for pandas and pyarrow (optimized for Lambda)
    // This layer includes: pandas, pyarrow, numpy, and other data processing libraries
    // See: https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html
    const awsDataWranglerLayer = lambda.LayerVersion.fromLayerVersionArn(
      this,
      "AWSDataWranglerLayer",
      `arn:aws:lambda:${cdk.Stack.of(this).region}:336392948345:layer:AWSSDKPandas-Python313:1`
    );

    // Custom layer for other dependencies (requests, websocket-client)
    const commonLayer = new lambda.LayerVersion(this, "CommonLayer", {
      code: lambda.Code.fromAsset("lambda/layers/common", {
        bundling: {
          image: lambda.Runtime.PYTHON_3_13.bundlingImage,
          command: [
            "bash",
            "-c",
            "pip install -r requirements.txt -t /asset-output/python && cp -r python/* /asset-output/python/",
          ],
        },
      }),
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_13],
      description: "Common Python dependencies for all Lambda functions",
    });

    const collectorRegionsJson = JSON.stringify(props.collectorRegions);

    const createLambdaFunction = (name: string, handlerPath: string): lambda.Function => {
      const fn = new lambda.Function(this, name, {
        runtime: lambda.Runtime.PYTHON_3_13,
        handler: "index.handler",
        code: lambda.Code.fromAsset(handlerPath),
        layers: [awsDataWranglerLayer, commonLayer],
        timeout: cdk.Duration.seconds(30),
        environment: {
          COLLECTORS_TABLE_NAME: props.collectorsTable.tableName,
          COLLECTOR_REGIONS: collectorRegionsJson,
          COLLECTOR_DEPLOYMENT_VERSION: props.collectorDeploymentVersion,
        },
      });
      props.collectorsTable.grantReadWriteData(fn);

      fn.addToRolePolicy(new iam.PolicyStatement({
        actions: [
          'ecs:*',
          'iam:PassRole',
          'logs:DescribeLogStreams',
          'logs:GetLogEvents',
        ],
        resources: ['*'],  // TODO: Restrict to specific resources
      }));

      return fn;
    };

    const createEndpoint = (config: EndpointConfig) => {
      const fn = createLambdaFunction(
        `${config.name}Function`,
        `lambda/functions/collectors/${config.handlerPath}`
      );

      const pathParts = config.path.split('/').filter(Boolean);
      let resource = this.api.root;
      for (const part of pathParts) {
        resource = resource.getResource(part) || resource.addResource(part);
      }

      const methodOptions: apigateway.MethodOptions = {
        apiKeyRequired: false,
        authorizationType: apigateway.AuthorizationType.COGNITO,
        authorizer: authorizer,
      };

      resource.addMethod(
        config.method,
        new apigateway.LambdaIntegration(fn),
        methodOptions,
      );
    };

    const endpoints: EndpointConfig[] = [
      {
        name: "CreateCollector",
        path: "collectors/create",
        method: "POST",
        handlerPath: "create",
      },
      {
        name: "ListCollectors",
        path: "collectors/list",
        method: "GET",
        handlerPath: "list",
      },
      {
        name: "GetCollector",
        path: "collectors/{listingId}",
        method: "GET",
        handlerPath: "get",
      },
      {
        name: "DeleteCollector",
        path: "collectors/delete",
        method: "DELETE",
        handlerPath: "delete",
      },
      {
        name: "RedeployCollectors",
        path: "collectors/redeploy",
        method: "POST",
        handlerPath: "redeploy",
      },
      {
        name: "CollectorLogs",
        path: "collectors/{listingId}/logs",
        method: "GET",
        handlerPath: "logs",
      },
    ];

    const transformJobsEndpoints: EndpointConfig[] = [
      {
        name: "ListTransformJobs",
        path: "transform-jobs/list",
        method: "GET",
        handlerPath: "transform-jobs/list",
      },
      {
        name: "SearchTransformJobs",
        path: "transform-jobs/search",
        method: "GET",
        handlerPath: "transform-jobs/search",
      },
    ];

    const gapsEndpoints: EndpointConfig[] = [
      {
        name: "ListGaps",
        path: "gaps/list",
        method: "GET",
        handlerPath: "gaps/list",
      },
      {
        name: "GetGapsByListing",
        path: "gaps/list/{listingId}",
        method: "GET",
        handlerPath: "gaps/get-by-listing",
      },
      {
        name: "UpdateGaps",
        path: "gaps/update",
        method: "POST",
        handlerPath: "gaps/update",
      },
    ];

    const coverageEndpoints: EndpointConfig[] = [
      {
        name: "GetCoverageSummary",
        path: "coverage/summary",
        method: "GET",
        handlerPath: "coverage/summary",
      },
      {
        name: "GetCoverageBySecurity",
        path: "coverage/security/{securityId}",
        method: "GET",
        handlerPath: "coverage/get-by-security",
      },
      {
        name: "GetCoverageBySecurityExchange",
        path: "coverage/{securityId}/{exchangeId}",
        method: "GET",
        handlerPath: "coverage/get-by-listing",
      },
    ];

    endpoints.forEach(createEndpoint);

    const createMarketDataLambda = (name: string, handlerPath: string): lambda.Function => {
      const fn = new lambda.Function(this, name, {
        runtime: lambda.Runtime.PYTHON_3_13,
        handler: "index.handler",
        code: lambda.Code.fromAsset(`lambda/functions/${handlerPath}`),
        layers: [awsDataWranglerLayer, commonLayer],
        timeout: cdk.Duration.seconds(30),
        environment: {
          TRANSFORM_JOBS_TABLE_NAME: props.transformJobsTable.tableName,
          GAPS_TABLE_NAME: props.gapsTable.tableName,
          FINAL_BUCKET_NAME: props.finalBucket.bucketName,
          METADATA_BUCKET_NAME: props.metadataBucket.bucketName,
          COVERAGE_TABLE_NAME: props.coverageTable.tableName,
          QUALITY_ISSUES_TABLE_NAME: props.qualityIssuesTable.tableName,
        },
      });

      props.transformJobsTable.grantReadWriteData(fn);
      props.gapsTable.grantReadWriteData(fn);
      props.finalBucket.grantRead(fn);
      props.metadataBucket.grantRead(fn);
      props.coverageTable.grantReadData(fn);
      props.qualityIssuesTable.grantReadWriteData(fn);

      return fn;
    };

    const createMarketDataEndpoint = (config: EndpointConfig) => {
      const fn = createMarketDataLambda(
        `${config.name}Function`,
        config.handlerPath
      );

      const pathParts = config.path.split('/').filter(Boolean);
      let resource = this.api.root;
      for (const part of pathParts) {
        resource = resource.getResource(part) || resource.addResource(part);
      }

      const methodOptions: apigateway.MethodOptions = {
        apiKeyRequired: false,
        authorizationType: apigateway.AuthorizationType.COGNITO,
        authorizer: authorizer,
      };

      resource.addMethod(
        config.method,
        new apigateway.LambdaIntegration(fn),
        methodOptions,
      );
    };

    const qualityIssuesEndpoints: EndpointConfig[] = [
      {
        name: "ListQualityIssues",
        path: "quality-issues/list",
        method: "GET",
        handlerPath: "quality-issues/list",
      },
      {
        name: "GetQualityIssuesByListing",
        path: "quality-issues/list/{listingId}",
        method: "GET",
        handlerPath: "quality-issues/get-by-listing",
      },
      {
        name: "UpdateQualityIssues",
        path: "quality-issues/update",
        method: "POST",
        handlerPath: "quality-issues/update",
      },
    ];

    transformJobsEndpoints.forEach(createMarketDataEndpoint);
    gapsEndpoints.forEach(createMarketDataEndpoint);
    coverageEndpoints.forEach(createMarketDataEndpoint);
    qualityIssuesEndpoints.forEach(createMarketDataEndpoint);

    // Listing statistics endpoint — read-only, only needs dailyListingStatisticsTable
    const listingStatsLambda = new lambda.Function(this, "GetListingStatisticsFunction", {
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/functions/listing-statistics/get"),
      layers: [commonLayer],
      timeout: cdk.Duration.seconds(30),
      environment: {
        LISTING_STATISTICS_TABLE_NAME: props.dailyListingStatisticsTable.tableName,
      },
    });
    props.dailyListingStatisticsTable.grantReadData(listingStatsLambda);

    const listingStatsResource = this.api.root
      .addResource("listing-statistics")
      .addResource("{listingId}");
    listingStatsResource.addMethod(
      "GET",
      new apigateway.LambdaIntegration(listingStatsLambda),
      { apiKeyRequired: false, authorizationType: apigateway.AuthorizationType.COGNITO, authorizer },
    );

    const listingStatsHistoryLambda = new lambda.Function(this, "GetListingStatisticsHistoryFunction", {
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/functions/listing-statistics/history"),
      layers: [commonLayer],
      timeout: cdk.Duration.seconds(30),
      environment: {
        LISTING_STATISTICS_TABLE_NAME: props.dailyListingStatisticsTable.tableName,
      },
    });
    props.dailyListingStatisticsTable.grantReadData(listingStatsHistoryLambda);
    listingStatsResource.addResource("history").addMethod(
      "GET",
      new apigateway.LambdaIntegration(listingStatsHistoryLambda),
      { apiKeyRequired: false, authorizationType: apigateway.AuthorizationType.COGNITO, authorizer },
    );

    // Quality backfill trigger — async invocation of the Java backfill Lambda
    const qualityBackfillTriggerLambda = new lambda.Function(this, "TriggerQualityBackfillFunction", {
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/functions/quality-issues/backfill"),
      layers: [commonLayer],
      timeout: cdk.Duration.seconds(30),
      environment: {
        QUALITY_BACKFILL_FUNCTION_NAME: props.qualityBackfillLambda.functionName,
      },
    });
    props.qualityBackfillLambda.grantInvoke(qualityBackfillTriggerLambda);

    const backfillResource = this.api.root
      .getResource("quality-issues")!
      .addResource("backfill");
    backfillResource.addMethod(
      "POST",
      new apigateway.LambdaIntegration(qualityBackfillTriggerLambda),
      { apiKeyRequired: false, authorizationType: apigateway.AuthorizationType.COGNITO, authorizer },
    );

    // Quality issue minute investigation — synchronous invocation of the Java investigation Lambda
    const qualityInvestigateLambda = new lambda.Function(this, "InvestigateQualityIssueFunction", {
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/functions/quality-issues/investigate"),
      layers: [commonLayer],
      timeout: cdk.Duration.seconds(60),
      environment: {
        INVESTIGATION_FUNCTION_NAME: props.qualityInvestigationLambda.functionName,
      },
    });
    props.qualityInvestigationLambda.grantInvoke(qualityInvestigateLambda);

    const investigateResource = this.api.root
      .getResource("quality-issues")!
      .addResource("investigate")
      .addResource("{listingId}");
    investigateResource.addMethod(
      "GET",
      new apigateway.LambdaIntegration(qualityInvestigateLambda),
      { apiKeyRequired: false, authorizationType: apigateway.AuthorizationType.COGNITO, authorizer },
    );

    const collectorEcsMonitorLambda = new lambda.Function(this, "CollectorEcsMonitorLambda", {
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "index.lambda_handler",
      code: lambda.Code.fromAsset("lambda/functions/collectors/ecs-monitor"),
      layers: [commonLayer],
      environment: {
        COLLECTORS_TABLE_NAME: props.collectorsTable.tableName,
        COLLECTOR_REGIONS: collectorRegionsJson,
      },
      timeout: cdk.Duration.seconds(30),
    });

    props.collectorsTable.grantReadWriteData(collectorEcsMonitorLambda);

    // Rule on the collector event bus that handles all ECS events (forwarded from all regions)
    const ecsMonitorRule = new events.Rule(this, "CollectorEcsMonitorRule", {
      eventBus: props.collectorEventBus,
      eventPattern: {
        source: ["aws.ecs"],
        detailType: ["ECS Task State Change"],
        detail: {
          lastStatus: ["RUNNING", "STOPPED", "PENDING"],
        },
      },
    });
    ecsMonitorRule.addTarget(new targets.LambdaFunction(collectorEcsMonitorLambda));


    new cdk.CfnOutput(this, "ApiUrl", {
      value: this.api.url,
      description: "API Gateway URL",
    });
  }
} 
