import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { Construct } from "constructs";
import * as path from "path";

export interface JavaLambdaProps {
  classPath: string;
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

    this.lambdaFunction = new lambda.Function(this, 'JavaLambda', {
      runtime: lambda.Runtime.JAVA_17,
      handler: `${props.classPath}::handleRequest`,
      code: lambda.Code.fromAsset(projectRoot, {
        bundling: {
          image: lambda.Runtime.JAVA_17.bundlingImage,
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
      memorySize: 2048,
      timeout: cdk.Duration.minutes(15),
      environment,
    });
  }
}