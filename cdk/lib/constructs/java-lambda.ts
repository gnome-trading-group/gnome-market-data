import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as logs from "aws-cdk-lib/aws-logs";
import * as ecrAssets from "aws-cdk-lib/aws-ecr-assets";
import { Construct } from "constructs";
import * as path from "path";
import * as fs from "fs";

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

    const dockerDir = this.buildDockerfile(props.name, props.classPath);

    const dockerImageAsset = new ecrAssets.DockerImageAsset(this, `${props.name}DockerImage`, {
      directory: projectRoot,
      file: path.relative(projectRoot, path.join(dockerDir, 'Dockerfile')),
      buildSecrets: {
        MAVEN_CREDENTIALS: 'env=MAVEN_CREDENTIALS',
      },
    });

    this.lambdaFunction = new lambda.DockerImageFunction(this, `${props.name}JavaLambda`, {
      functionName: `${cdk.Aws.STACK_NAME}-${props.name}`,
      code: lambda.DockerImageCode.fromEcr(dockerImageAsset.repository, {
        tagOrDigest: dockerImageAsset.assetHash,
      }),
      memorySize: props.memorySize ?? 2048,
      timeout: cdk.Duration.minutes(15),
      environment,
      logRetention: logs.RetentionDays.ONE_MONTH,
    });
  }

  private buildDockerfile(name: string, classPath: string): string {
    const dockerDir = path.join(__dirname, `java-lambda-docker-${name}`);

    if (!fs.existsSync(dockerDir)) {
      fs.mkdirSync(dockerDir);
    }

    // Multi-stage Dockerfile: build stage + runtime stage
    const dockerfileContent = `
# Build stage
FROM ubuntu:24.04 AS build

RUN apt-get update && apt-get install -y openjdk-17-jdk maven jq

WORKDIR /build

COPY pom.xml .
COPY settings.xml .
COPY src ./src

RUN --mount=type=secret,id=MAVEN_CREDENTIALS \
    export MAVEN_CREDENTIALS=$(cat /run/secrets/MAVEN_CREDENTIALS) && \
    export GITHUB_ACTOR=$(echo $MAVEN_CREDENTIALS | jq -r '.GITHUB_ACTOR') && \
    export GITHUB_TOKEN=$(echo $MAVEN_CREDENTIALS | jq -r '.GITHUB_TOKEN') && \
    mvn clean package -s settings.xml && \
    mvn dependency:copy-dependencies -DincludeScope=runtime

# Runtime stage - ubuntu:24.04 for GLIBC requirements
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /function

COPY --from=build /build/target/gnome-market-data-1.0.0-SNAPSHOT.jar ./
COPY --from=build /build/target/dependency/*.jar ./

# Set runtime interface client as default command for the container runtime
# The runtime interface client is included as a dependency in the project
ENTRYPOINT [ "/usr/bin/java", "-cp", "./*", "com.amazonaws.services.lambda.runtime.api.client.AWSLambda" ]

CMD [ "${classPath}::handleRequest" ]
`.trim();

    fs.writeFileSync(path.join(dockerDir, 'Dockerfile'), dockerfileContent);
    return dockerDir;
  }
}