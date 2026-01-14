import * as cdk from "aws-cdk-lib";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
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
        layers: [commonLayer],
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

    endpoints.forEach(createEndpoint);

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


    // TODO: Create the Java endpoints for market data related things

    new cdk.CfnOutput(this, "ApiUrl", {
      value: this.api.url,
      description: "API Gateway URL",
    });
  }
} 
