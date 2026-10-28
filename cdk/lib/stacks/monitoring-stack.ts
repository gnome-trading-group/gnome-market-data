import * as cdk from "aws-cdk-lib";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { Construct } from 'constructs';
import { DefaultDashboardFactory, MonitoringFacade, noopAction } from "cdk-monitoring-constructs";
import { MarketDataConfig } from "../config";

export interface MonitoringStackProps extends cdk.StackProps {
  config: MarketDataConfig;
  api: apigateway.RestApi;
  reconcilerLambda: lambda.Function;
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
      });
  }
}
