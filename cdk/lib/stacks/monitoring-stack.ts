import * as cdk from "aws-cdk-lib";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as cw from 'aws-cdk-lib/aws-cloudwatch';
import { Construct } from 'constructs';
import { CustomMetricGroup, DefaultDashboardFactory, MonitoringFacade, noopAction } from "cdk-monitoring-constructs";
import { COLLECTOR_METRICS_NAMESPACE, COLLECTOR_ERROR_METRIC_NAME } from "./collector-regional-stack";
import { MarketDataConfig } from "../config";

export interface MonitoringStackProps extends cdk.StackProps {
  config: MarketDataConfig;
  collectorRegions: string[];
  api: apigateway.RestApi;
  gapLambda: lambda.Function;
  gapQueue: sqs.Queue;
  mergerLambda: lambda.Function;
  mergerQueue: sqs.Queue;
  transformerJobCreatorLambda: lambda.Function;
  transformerQueue: sqs.Queue;
  transformerJobProcessorLambda: lambda.Function;
  inventoryProcessorLambda: lambda.Function;
  qualityCheckLambda: lambda.Function;
  qualityCheckQueue: sqs.Queue;
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

    const collectorLogMetrics: CustomMetricGroup[] = [];
    for (const region of props.collectorRegions) {
      collectorLogMetrics.push(
        {
          metrics: [
            new cw.Metric({
              namespace: COLLECTOR_METRICS_NAMESPACE,
              metricName: `${COLLECTOR_ERROR_METRIC_NAME}-${region}`,
              region: region,
              statistic: 'Sum',
              period: cdk.Duration.minutes(1),
            }),
          ],
          title: region,
        }
      );
    }

    const lambdaAlarms: Record<string, any> = {
      addFaultCountAlarm: {
        Error: { maxErrorCount: 0, },
      },
    };

    monitoring
      .monitorCustom({
        alarmFriendlyName: 'CollectorECSLogErrors',
        humanReadableName: 'Collector ECS Log Errors',
        metricGroups: collectorLogMetrics,
      })
      .monitorApiGateway({
        api: props.api,
        humanReadableName: 'MarketData API',
        alarmFriendlyName: 'MarketDataApi',
      })
      .monitorLambdaFunction({
        lambdaFunction: props.mergerLambda,
        humanReadableName: 'Merger Lambda',
        alarmFriendlyName: 'MergerLambda',
        ...lambdaAlarms,
      })
      .monitorSqsQueue({
        queue: props.mergerQueue,
        humanReadableName: 'Merger Queue',
        alarmFriendlyName: 'MergerQueue',
      })
      .monitorLambdaFunction({
        lambdaFunction: props.transformerJobCreatorLambda,
        humanReadableName: 'Transformer Job Creator Lambda',
        alarmFriendlyName: 'TransformerJobCreatorLambda',
        ...lambdaAlarms,
      })
      .monitorSqsQueue({
        queue: props.transformerQueue,
        humanReadableName: 'Transformer Queue',
        alarmFriendlyName: 'TransformerQueue',
      })
      .monitorLambdaFunction({
        lambdaFunction: props.transformerJobProcessorLambda,
        humanReadableName: 'Transformer Job Processor Lambda',
        alarmFriendlyName: 'TransformerJobProcessorLambda',
        ...lambdaAlarms,
      })
      .monitorLambdaFunction({
        lambdaFunction: props.gapLambda,
        humanReadableName: 'Gap Detector Lambda',
        alarmFriendlyName: 'GapDetectorLambda',
        ...lambdaAlarms,
      })
      .monitorSqsQueue({
        queue: props.gapQueue,
        humanReadableName: 'Gap Detector Queue',
        alarmFriendlyName: 'GapDetectorQueue',
      })
      .monitorLambdaFunction({
        lambdaFunction: props.inventoryProcessorLambda,
        humanReadableName: 'Inventory Processor Lambda',
        alarmFriendlyName: 'InventoryProcessorLambda',
        ...lambdaAlarms,
      })
      .monitorLambdaFunction({
        lambdaFunction: props.qualityCheckLambda,
        humanReadableName: 'Quality Check Lambda',
        alarmFriendlyName: 'QualityCheckLambda',
        ...lambdaAlarms,
      })
      .monitorSqsQueue({
        queue: props.qualityCheckQueue,
        humanReadableName: 'Quality Check Queue',
        alarmFriendlyName: 'QualityCheckQueue',
      });
  }
}
