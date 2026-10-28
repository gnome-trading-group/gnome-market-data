import * as cdk from "aws-cdk-lib";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { API_KEY_SECRET_NAMES, ApiKeySecret, CallerKind, GnomeApiAuthorizer } from "@gnome-trading-group/gnome-shared-cdk";
import { Construct } from "constructs";

interface BackendStackProps extends cdk.StackProps {
  gapsTable: dynamodb.ITable;
  finalBucket: s3.IBucket;
  metadataBucket: s3.IBucket;
  coverageTable: dynamodb.ITable;
  collectionRulesTable: dynamodb.ITable;
  collectionTargetsTable: dynamodb.ITable;
  collectionShardsTable: dynamodb.ITable;
  collectionCoverageTable: dynamodb.ITable;
  collectionGapsTable: dynamodb.ITable;
  reconcilerQueue: sqs.IQueue;
  collectionRegions: string[];
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

    // Systems that drive collection (pushing events into rules) call with this key. Not bound to the stage, which would
    // be a cycle: the stage depends on every method, the methods on the authorizer and the authorizer on this key.
    const apiKey = new apigateway.ApiKey(this.api, "MarketDataApiKey", { apiKeyName: "market-data-api-key" });
    new ApiKeySecret(this, "MarketDataApiKeySecret", { apiKey, secretName: API_KEY_SECRET_NAMES.MARKET_DATA });
    // One authorizer for every route: people by Cognito ID token, services by the key above, bots by access token.
    const auth = new GnomeApiAuthorizer(this, "Authorizer", { userPool, apiKeys: [apiKey] });
    const personOnly: apigateway.MethodOptions = {
      authorizationType: apigateway.AuthorizationType.CUSTOM,
      authorizer: auth.authorizerFor([CallerKind.PERSON]),
    };

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

    const route = (path: string, method: string, fn: lambda.IFunction, kinds: CallerKind[] = [CallerKind.PERSON]) => {
      auth.addRoute(this.api.root, path, new apigateway.LambdaIntegration(fn), { [method]: kinds });
    };

    this.collectionRoutes(props, commonLayer, route);

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
        name: "GetCoverageByStream",
        path: "coverage/stream/{streamId}",
        method: "GET",
        handlerPath: "coverage/get-by-stream",
      },
    ];

    const createMarketDataLambda = (name: string, handlerPath: string): lambda.Function => {
      const fn = new lambda.Function(this, name, {
        runtime: lambda.Runtime.PYTHON_3_13,
        handler: "index.handler",
        code: lambda.Code.fromAsset(`lambda/functions/${handlerPath}`),
        layers: [commonLayer],
        timeout: cdk.Duration.seconds(30),
        environment: {
          GAPS_TABLE_NAME: props.gapsTable.tableName,
          FINAL_BUCKET_NAME: props.finalBucket.bucketName,
          METADATA_BUCKET_NAME: props.metadataBucket.bucketName,
          COVERAGE_TABLE_NAME: props.coverageTable.tableName,
        },
      });

      props.gapsTable.grantReadWriteData(fn);
      props.finalBucket.grantRead(fn);
      props.metadataBucket.grantRead(fn);
      props.coverageTable.grantReadData(fn);

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

      resource.addMethod(config.method, new apigateway.LambdaIntegration(fn), personOnly);
    };

    gapsEndpoints.forEach(createMarketDataEndpoint);
    coverageEndpoints.forEach(createMarketDataEndpoint);

    new cdk.CfnOutput(this, "ApiUrl", {
      value: this.api.url,
      description: "API Gateway URL",
    });
  }

  /**
   * The collection control plane's API: rules people edit, the reconciler's targets and shards to read, a trigger that
   * ticks it now, and coverage. Every rule change wakes the reconciler through its queue; nothing here writes targets
   * or shards, which only the reconciler does.
   */
  private collectionRoutes(
    props: BackendStackProps,
    commonLayer: lambda.ILayerVersion,
    route: (path: string, method: string, fn: lambda.IFunction, kinds?: CallerKind[]) => void,
  ) {
    const environment = {
      COLLECTION_RULES_TABLE: props.collectionRulesTable.tableName,
      COLLECTION_TARGETS_TABLE: props.collectionTargetsTable.tableName,
      COLLECTION_SHARDS_TABLE: props.collectionShardsTable.tableName,
      COLLECTION_COVERAGE_TABLE: props.collectionCoverageTable.tableName,
      COLLECTION_GAPS_TABLE: props.collectionGapsTable.tableName,
      METADATA_BUCKET_NAME: props.metadataBucket.bucketName,
      RECONCILER_QUEUE_URL: props.reconcilerQueue.queueUrl,
    };
    const handler = (id: string, dir: string) => new lambda.Function(this, id, {
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "index.handler",
      code: lambda.Code.fromAsset(`lambda/functions/collection/${dir}`),
      layers: [commonLayer],
      memorySize: 512,
      timeout: cdk.Duration.seconds(30),
      environment,
    });

    const rules = handler("CollectionRulesFunction", "rules");
    props.collectionRulesTable.grantReadWriteData(rules);
    props.reconcilerQueue.grantSendMessages(rules);
    // Services too: other systems drive collection by creating rules and adding events to them.
    const peopleAndServices = [CallerKind.PERSON, CallerKind.SERVICE];
    route("collection/rules", "GET", rules, peopleAndServices);
    route("collection/rules", "POST", rules, peopleAndServices);
    route("collection/rules/{ruleId}", "PATCH", rules, peopleAndServices);
    route("collection/rules/{ruleId}", "DELETE", rules, peopleAndServices);

    route("collection/rule-types", "GET", handler("CollectionRuleTypesFunction", "rule-types"));

    // Asks the registry what a draft rule would select, as the reconciler would.
    const preview = handler("CollectionRulePreviewFunction", "rule-preview");
    preview.addEnvironment("REGISTRY_API_URL", cdk.Fn.importValue("RegistryApiUrl"));
    preview.addEnvironment("REGISTRY_API_KEY_SECRET", API_KEY_SECRET_NAMES.REGISTRY);
    secretsmanager.Secret.fromSecretNameV2(this, "RegistryApiKeySecret", API_KEY_SECRET_NAMES.REGISTRY)
      .grantRead(preview);
    route("collection/rule-preview", "POST", preview);

    const triggers = handler("CollectionTriggersFunction", "triggers");
    props.reconcilerQueue.grantSendMessages(triggers);
    route("collection/triggers", "POST", triggers);

    const targets = handler("CollectionTargetsFunction", "targets");
    props.collectionTargetsTable.grantReadData(targets);
    route("collection/targets", "GET", targets);

    const shards = handler("CollectionShardsFunction", "shards");
    props.collectionShardsTable.grantReadData(shards);
    route("collection/shards", "GET", shards);

    const shard = handler("CollectionShardFunction", "shard");
    props.collectionShardsTable.grantReadData(shard);
    props.metadataBucket.grantRead(shard, "collection/shards/*");
    route("collection/shards/{shardId}", "GET", shard);

    const shardLogs = handler("CollectionShardLogsFunction", "shard-logs");
    shardLogs.addToRolePolicy(new iam.PolicyStatement({
      actions: ["logs:GetLogEvents"],
      resources: props.collectionRegions.map(
        (region) => `arn:aws:logs:${region}:${this.account}:log-group:/collection/shards-${region}:*`),
    }));
    route("collection/shards/{shardId}/logs", "GET", shardLogs);

    const coverage = handler("CollectionCoverageFunction", "coverage");
    props.collectionCoverageTable.grantReadData(coverage);
    props.collectionGapsTable.grantReadData(coverage);
    route("collection/coverage/{streamId}", "GET", coverage);
  }
}
