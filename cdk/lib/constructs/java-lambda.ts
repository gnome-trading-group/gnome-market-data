import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";
import * as path from "path";

export interface JavaLambdaProps {
  name: string;
  classPath: string;
  memorySize?: number;
  environment?: { [key: string]: string };
}

export class JavaLambda extends Construct {

  private readonly DEFAULT_JVM_OPTIONS = " --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED";
  public readonly lambdaFunction: lambda.Function;

  constructor(scope: Construct, id: string, props: JavaLambdaProps) {
    super(scope, id);

    const projectRoot = path.join(__dirname, '..', '..', '..');

    const environment: { [key: string]: string } = {
      ...(props.environment || {}),
    };

    environment.JAVA_TOOL_OPTIONS = `${environment.JAVA_TOOL_OPTIONS ?? ''}${this.DEFAULT_JVM_OPTIONS}`;

    this.lambdaFunction = new lambda.Function(this, `${props.name}JavaLambda`, {
      runtime: lambda.Runtime.JAVA_21,
      functionName: `${cdk.Aws.STACK_NAME}-${props.name}`,
      handler: `${props.classPath}::handleRequest`,
      code: lambda.Code.fromAsset(projectRoot, {
        bundling: {
          image: lambda.Runtime.JAVA_21.bundlingImage,
          command: [
            '/bin/sh',
            '-c',
            [
              'export GITHUB_ACTOR=$(echo $MAVEN_CREDENTIALS | jq -r \'.GITHUB_ACTOR\')',
              'export GITHUB_TOKEN=$(echo $MAVEN_CREDENTIALS | jq -r \'.GITHUB_TOKEN\')',
              'mvn clean package -s settings.xml',
              'cp target/gnome-market-data-1.0.0-SNAPSHOT.jar /asset-output/merger-lambda.jar',
            ].join(' && '),
          ],
          environment: {
            MAVEN_CREDENTIALS: process.env.MAVEN_CREDENTIALS || '',
          },
        },
      }),
      memorySize: props.memorySize ?? 2048,
      timeout: cdk.Duration.minutes(15),
      environment,
      logRetention: logs.RetentionDays.ONE_MONTH,
    });
  }
}