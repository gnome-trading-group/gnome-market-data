import * as cdk from "aws-cdk-lib";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as cognito from "aws-cdk-lib/aws-cognito";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as ssm from "aws-cdk-lib/aws-ssm";
import {
  API_KEY_SECRET_NAMES,
  ApiKeySecret,
  CallerKind,
  GnomeApiAuthorizer,
  SERVICE_ENDPOINTS,
} from "@gnome-trading-group/gnome-shared-cdk";
import { Construct } from "constructs";

interface BackendStackProps extends cdk.StackProps {
  metadataBucket: s3.IBucket;
  collectionRulesTable: dynamodb.ITable;
  collectionTargetsTable: dynamodb.ITable;
  collectionShardsTable: dynamodb.ITable;
  collectionCoverageTable: dynamodb.ITable;
  collectionGapsTable: dynamodb.ITable;
  collectionLeasesTable: dynamodb.ITable;
  reconcilerQueue: sqs.IQueue;
  collectionRegions: string[];
  coverageTrackedSinceNs: string;
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

    new cdk.CfnOutput(this, "ApiUrl", {
      value: this.api.url,
      description: "API Gateway URL",
    });
    // The registry's launcher reads this to create sessions' leases; a CloudFormation export would tie the two repos'
    // stacks into a cycle.
    new ssm.StringParameter(this, "ApiUrlParameter", {
      parameterName: "/gnome/market-data/api-url",
      stringValue: this.api.url,
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

    const registryKey = secretsmanager.Secret.fromSecretNameV2(this, "RegistryApiKeySecret", API_KEY_SECRET_NAMES.REGISTRY);
    const readsRegistry = (fn: lambda.Function) => {
      fn.addEnvironment("REGISTRY_API_URL", cdk.Fn.importValue("RegistryApiUrl"));
      fn.addEnvironment("REGISTRY_API_KEY_SECRET", API_KEY_SECRET_NAMES.REGISTRY);
      registryKey.grantRead(fn);
    };

    // Asks the registry what a draft rule would select, as the reconciler would.
    const preview = handler("CollectionRulePreviewFunction", "rule-preview");
    readsRegistry(preview);
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

    // Coverage is read by backtests (services, by key) and the bot as well as people.
    const readers = [CallerKind.PERSON, CallerKind.SERVICE, CallerKind.BOT];
    const readsCoverage = (fn: lambda.Function) => {
      props.collectionCoverageTable.grantReadData(fn);
      props.collectionGapsTable.grantReadData(fn);
      props.collectionTargetsTable.grantReadData(fn);
    };
    const coverage = handler("CollectionCoverageFunction", "coverage");
    readsCoverage(coverage);
    route("collection/coverage/{streamId}", "GET", coverage, readers);

    // Sessions' leases: the registry's launcher creates them (by key), sessions and people read them, and the
    // reconciler places what they want. GET joins the lease to targets, the shards' latest statuses and their
    // service-endpoints items, so a session needs no DynamoDB access of its own.
    const leases = handler("CollectionLeasesFunction", "leases");
    leases.addEnvironment("COLLECTION_LEASES_TABLE", props.collectionLeasesTable.tableName);
    leases.addEnvironment("SERVICE_ENDPOINTS_TABLE", SERVICE_ENDPOINTS.tableName);
    leases.addEnvironment("SERVICE_ENDPOINTS_REGION", SERVICE_ENDPOINTS.region);
    props.collectionLeasesTable.grantReadWriteData(leases);
    props.collectionTargetsTable.grantReadData(leases);
    props.metadataBucket.grantRead(leases, "collection/shards/*");
    props.reconcilerQueue.grantSendMessages(leases);
    leases.addToRolePolicy(new iam.PolicyStatement({
      actions: ["dynamodb:BatchGetItem", "dynamodb:GetItem"],
      resources: [
        `arn:aws:dynamodb:${SERVICE_ENDPOINTS.region}:${this.account}:table/${SERVICE_ENDPOINTS.tableName}`,
      ],
    }));
    route("collection/leases", "POST", leases, peopleAndServices);
    route("collection/leases/{sessionId}", "GET", leases, peopleAndServices);
    route("collection/leases/{sessionId}", "DELETE", leases, peopleAndServices);

    const missing = handler("CollectionMissingFunction", "missing");
    readsCoverage(missing);
    readsRegistry(missing);
    missing.addEnvironment("COVERAGE_TRACKED_SINCE_NS", props.coverageTrackedSinceNs);
    route("collection/missing", "GET", missing, readers);
  }
}
