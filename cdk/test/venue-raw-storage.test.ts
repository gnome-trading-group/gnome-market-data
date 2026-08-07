import * as cdk from 'aws-cdk-lib';
import * as events from 'aws-cdk-lib/aws-events';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { Stage } from '@gnome-trading-group/gnome-shared-cdk';
import { CONFIGS } from '../lib/config';
import { CollectorRegionalStack } from '../lib/stacks/collector-regional-stack';
import { StorageStack } from '../lib/stacks/storage-stack';

const config = CONFIGS[Stage.DEV]!;
const account = config.account.environment.account!;
const region = 'us-east-1';
const venueRawBucketName = 'gnome-market-data-venue-raw-dev';

describe('venue-raw storage', () => {
  test('creates an isolated TLS-only bucket without merger notifications', () => {
    const app = new cdk.App();
    const stack = new StorageStack(app, 'Storage', {
      env: { account, region },
      config,
    });
    const template = Template.fromStack(stack);

    template.hasResourceProperties('AWS::S3::Bucket', {
      BucketName: venueRawBucketName,
      BucketEncryption: {
        ServerSideEncryptionConfiguration: [
          { ServerSideEncryptionByDefault: { SSEAlgorithm: 'AES256' } },
        ],
      },
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      },
    });
    template.hasResourceProperties('AWS::S3::BucketPolicy', {
      PolicyDocument: {
        Statement: Match.arrayWith([
          Match.objectLike({
            Action: 's3:*',
            Condition: { Bool: { 'aws:SecureTransport': 'false' } },
            Effect: 'Deny',
          }),
        ]),
      },
    });

    const venueRawBucket = Object.entries(template.findResources('AWS::S3::Bucket'))
      .find(([, resource]) => resource.Properties?.BucketName === venueRawBucketName);
    expect(venueRawBucket).toBeDefined();

    const notificationConfiguration = JSON.stringify(
      template.findResources('Custom::S3BucketNotifications'),
    );
    expect(notificationConfiguration).not.toContain(venueRawBucket![0]);
  });

  test('passes the bucket to the collector and grants its task role put-only access', () => {
    const app = new cdk.App();
    const busStack = new cdk.Stack(app, 'Bus', { env: { account, region } });
    const primaryEventBus = new events.EventBus(busStack, 'PrimaryEventBus', {
      eventBusName: 'collector-ecs-events',
    });
    const stack = new CollectorRegionalStack(app, 'Collector', {
      env: { account, region },
      config,
      deploymentRegion: region,
      rawBucketName: 'gnome-market-data-raw-dev',
      venueRawBucketName,
      registryApiKeyId: 'test-registry-key-id',
      registryApiKeyArn: 'arn:aws:apigateway:us-east-1::/apikeys/test-registry-key-id',
      primaryEventBus,
    });
    const template = Template.fromStack(stack);

    template.hasResourceProperties('AWS::ECS::TaskDefinition', {
      ContainerDefinitions: Match.arrayWith([
        Match.objectLike({
          Environment: Match.arrayWith([
            { Name: 'VENUE_RAW_BUCKET', Value: venueRawBucketName },
          ]),
        }),
      ]),
    });

    const venueRawStatements = Object.values(template.findResources('AWS::IAM::Policy'))
      .flatMap(resource => resource.Properties.PolicyDocument.Statement)
      .filter(statement => JSON.stringify(statement.Resource).includes(venueRawBucketName));
    expect(venueRawStatements).toHaveLength(1);

    const actions = Array.isArray(venueRawStatements[0].Action)
      ? venueRawStatements[0].Action
      : [venueRawStatements[0].Action];
    expect(actions).toContain('s3:PutObject');
    expect(actions).not.toContain('s3:GetObject*');
    expect(actions).not.toContain('s3:DeleteObject*');
  });
});
