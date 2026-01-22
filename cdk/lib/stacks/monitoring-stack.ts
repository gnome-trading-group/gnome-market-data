import * as cdk from "aws-cdk-lib";
import * as apigateway from "aws-cdk-lib/aws-apigateway";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from 'aws-cdk-lib/aws-sns';
import * as cw from 'aws-cdk-lib/aws-cloudwatch';
import { Construct } from 'constructs';
import { CustomMetricGroup, MonitoringFacade, SnsAlarmActionStrategy } from "cdk-monitoring-constructs";
import { COLLECTOR_METRICS_NAMESPACE, COLLECTOR_ERROR_METRIC_NAME } from "./collector-regional-stack";
import { MarketDataConfig } from "../config";
import { Stage } from "@gnome-trading-group/gnome-shared-cdk";

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
}

export class MonitoringStack extends cdk.Stack {

  constructor(scope: Construct, id: string, props: MonitoringStackProps) {
    super(scope, id, props);

    const slackSnsTopic = sns.Topic.fromTopicArn(
      this, 'ImportedSlackSnsTopic', cdk.Fn.importValue('SlackSnsTopicArn')
    );

    const monitoring = new MonitoringFacade(this, 'MarketDataDashboard', {
      alarmFactoryDefaults: {
        actionsEnabled: props.config.account.stage === Stage.PROD, // Only enable alarms in prod
        alarmNamePrefix: 'MarketData-',
        action: new SnsAlarmActionStrategy({ onAlarmTopic: slackSnsTopic }),
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
        Critical: { maxErrorCount: 0, },
      },
    };

    monitoring
      .addLargeHeader('Gnome MarketData')
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
      });
  }
}
