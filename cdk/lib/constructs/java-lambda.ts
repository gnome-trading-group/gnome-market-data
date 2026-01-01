import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { Construct } from "constructs";
import { JAVA_LAMBDA_JAR_PATH } from "../config";

export interface JavaLambdaProps {
  classPath: string;
  environment: { [key: string]: string };
}

export class JavaLambda extends Construct {
  public readonly lambdaFunction: lambda.Function;

  constructor(scope: Construct, id: string, props: JavaLambdaProps) {
    super(scope, id);

    this.lambdaFunction = new lambda.Function(this, 'JavaLambda', {
      runtime: lambda.Runtime.JAVA_17,
      handler: `${props.classPath}::handleRequest`,
      code: lambda.Code.fromAsset(JAVA_LAMBDA_JAR_PATH),
      memorySize: 2048,
      timeout: cdk.Duration.minutes(15),
      environment: props.environment,
    });
  }
}