import * as cdk from "aws-cdk-lib";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { Construct } from 'constructs';
import { DefaultDashboardFactory, MonitoringFacade, noopAction } from "cdk-monitoring-constructs";
import { MarketDataConfig } from "../config";

export interface MonitoringStackProps extends cdk.StackProps {
  config: MarketDataConfig;
  api: apigateway.RestApi;
  reconcilerLambda: lambda.Function;
  collectionRegions: string[];
}

export class MonitoringStack extends cdk.Stack {

  constructor(scope: Construct, id: string, props: MonitoringStackProps) {
    super(scope, id, props);

    // The alarms carry no action: the registry's alerting stack hears every alarm change through EventBridge, and
    // routes dev's to the dev channel. The pipeline is batch work behind trading, so none of it pages.
    const monitoring = new MonitoringFacade(this, 'MarketDataDashboard', {
      // Only the alarms are used; each dashboard is billed monthly and nobody reads them.
      dashboardFactory: new DefaultDashboardFactory(this, 'MarketDataDashboardFactory', {
        dashboardNamePrefix: 'MarketDataDashboard',
        createDashboard: false,
      }),
      alarmFactoryDefaults: {
        actionsEnabled: true,
        alarmNamePrefix: 'MarketData-',
        action: noopAction(),
        datapointsToAlarm: 1,
      },
    });

    monitoring
      .monitorApiGateway({
        api: props.api,
        humanReadableName: 'MarketData API',
        alarmFriendlyName: 'MarketDataApi',
      })
      // RECONCILER_FAILING: a tick that throws fails its batch and the next minute's tick retries, so one failure is
      // noise; three in the alarm's period mean placement and coverage have stopped.
      .monitorLambdaFunction({
        lambdaFunction: props.reconcilerLambda,
        humanReadableName: 'Collection Reconciler Lambda',
        alarmFriendlyName: 'ReconcilerLambda',
        addFaultCountAlarm: {
          Error: { maxErrorCount: 2 },
        },
      })
      // DIGEST_FAILING: the digest posts nothing on a quiet day, so a digest that fails would look like one.
      .monitorLambdaFunction({
        lambdaFunction: this.shardLogDigest(props.collectionRegions),
        humanReadableName: 'Shard Log Digest Lambda',
        alarmFriendlyName: 'ShardLogDigestLambda',
        addFaultCountAlarm: {
          Error: { maxErrorCount: 0 },
        },
      });
  }

  /**
   * SHARD_LOG_DIGEST: once a day, the shards' unexpected errors of the last 24 h by kind, against the day before, so a
   * new kind is seen even when no alert's threshold is met. Posts nothing on a day without any.
   */
  private shardLogDigest(regions: string[]): lambda.Function {
    const digest = new lambda.Function(this, "ShardLogDigestFunction", {
      runtime: lambda.Runtime.PYTHON_3_13,
      handler: "index.handler",
      code: lambda.Code.fromAsset("lambda/functions/monitoring/shard-log-digest"),
      memorySize: 256,
      // One Logs Insights query per region, each over two days of shard logs (about half a GB).
      timeout: cdk.Duration.minutes(5),
      environment: { COLLECTION_REGIONS: regions.join(",") },
    });
    digest.addToRolePolicy(new iam.PolicyStatement({
      actions: ["logs:StartQuery"],
      resources: regions.map(region => `arn:aws:logs:${region}:${this.account}:log-group:/collection/shards-${region}:*`),
    }));
    // GetQueryResults names no log group, so it can't be scoped to one.
    digest.addToRolePolicy(new iam.PolicyStatement({ actions: ["logs:GetQueryResults"], resources: ["*"] }));
    digest.addToRolePolicy(new iam.PolicyStatement({
      actions: ["events:PutEvents"],
      resources: [`arn:aws:events:${this.region}:${this.account}:event-bus/gnome-alerts`],
    }));
    new events.Rule(this, "ShardLogDigestSchedule", {
      // Morning in New York.
      schedule: events.Schedule.cron({ minute: "0", hour: "13" }),
      targets: [new targets.LambdaFunction(digest)],
    });
    return digest;
  }
}
