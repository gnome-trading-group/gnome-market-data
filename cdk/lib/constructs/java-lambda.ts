import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { Construct } from "constructs";
import * as path from "path";

export interface JavaLambdaProps {
  classPath: string;
  environment: { [key: string]: string };
}

export class JavaLambda extends Construct {
  public readonly lambdaFunction: lambda.Function;

  constructor(scope: Construct, id: string, props: JavaLambdaProps) {
    super(scope, id);

    const projectRoot = path.join(__dirname, '..', '..', '..');

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
              // Extract GitHub credentials from the secret
              'echo $MAVEN_CREDENTIALS | jq -r \'.GITHUB_ACTOR\'',
              'echo $MAVEN_CREDENTIALS | jq -r \'.GITHUB_TOKEN\'',
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
      environment: props.environment,
    });
  }
}