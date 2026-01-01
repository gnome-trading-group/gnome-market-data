import * as cdk from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

export const COLLECTOR_EVENT_BUS_NAME = "collector-ecs-events";

export class EventBusStack extends cdk.Stack {
  public readonly collectorEventBus: events.EventBus;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.collectorEventBus = new events.EventBus(this, "CollectorEventBus", {
      eventBusName: COLLECTOR_EVENT_BUS_NAME,
    });

    this.collectorEventBus.addToResourcePolicy(new iam.PolicyStatement({
      sid: "AllowSameAccountPutEvents",
      effect: iam.Effect.ALLOW,
      principals: [new iam.AccountPrincipal(this.account)],
      actions: ["events:PutEvents"],
      resources: [this.collectorEventBus.eventBusArn],
    }));

    new cdk.CfnOutput(this, "CollectorEventBusArn", {
      value: this.collectorEventBus.eventBusArn,
      description: "Collector Event Bus ARN for cross-region event forwarding",
    });
  }
}

