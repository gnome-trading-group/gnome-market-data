import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { SandboxCollectorStack } from '../lib/sandbox-collector-stack';

describe('sandbox collector stack', () => {
  const app = new cdk.App();
  const stack = new SandboxCollectorStack(app, 'Sandbox', {
    env: { account: '111111111111', region: 'us-east-1' },
  });
  const template = Template.fromStack(stack);

  test('defaults the service off and limits it to one task', () => {
    template.hasParameter('DesiredCount', {
      Type: 'Number',
      Default: 0,
      AllowedValues: ['0', '1'],
    });
    template.hasResourceProperties('AWS::ECS::Service', {
      DesiredCount: { Ref: 'DesiredCount' },
      LaunchType: 'FARGATE',
      NetworkConfiguration: {
        AwsvpcConfiguration: Match.objectLike({
          AssignPublicIp: 'ENABLED',
        }),
      },
    });
  });

  test('uses small Fargate sizing and injects the registry key as a secret', () => {
    template.hasParameter('ListingId', {
      Type: 'String',
      Default: '',
      AllowedPattern: '^$|^[0-9]+$',
    });
    template.hasParameter('ListingIds', {
      Type: 'String',
      Default: '',
      AllowedPattern: '^$|^[0-9]+( *, *[0-9]+)*$',
    });
    expect(template.toJSON().Rules.RequireListingSelection.Assertions).toEqual([
      {
        Assert: {
          'Fn::Or': [
            { 'Fn::Not': [{ 'Fn::Equals': [{ Ref: 'ListingIds' }, ''] }] },
            { 'Fn::Not': [{ 'Fn::Equals': [{ Ref: 'ListingId' }, ''] }] },
          ],
        },
        AssertDescription: 'Provide ListingIds or the legacy ListingId parameter.',
      },
    ]);
    template.hasResourceProperties('AWS::ECS::TaskDefinition', {
      Cpu: '256',
      Memory: '512',
      RuntimePlatform: {
        CpuArchitecture: 'X86_64',
        OperatingSystemFamily: 'LINUX',
      },
      ContainerDefinitions: Match.arrayWith([
        Match.objectLike({
          Environment: Match.arrayWith([
            {
              Name: 'LISTINGS',
              Value: {
                'Fn::If': ['HasListingIds', { Ref: 'ListingIds' }, { Ref: 'ListingId' }],
              },
            },
            { Name: 'POLYMARKET_WS_URL', Value: 'wss://ws-subscriptions-clob.polymarket.com/ws/market' },
            { Name: 'STAGE', Value: 'dev' },
          ]),
          Secrets: Match.arrayWith([
            Match.objectLike({
              Name: 'REGISTRY_API_KEY',
              ValueFrom: { Ref: 'RegistryApiKeySecretArn' },
            }),
          ]),
        }),
      ]),
    });
  });

  test('creates two private encrypted TLS-only buckets with bounded retention', () => {
    template.resourceCountIs('AWS::S3::Bucket', 2);
    template.allResourcesProperties('AWS::S3::Bucket', Match.objectLike({
      BucketEncryption: {
        ServerSideEncryptionConfiguration: [
          { ServerSideEncryptionByDefault: { SSEAlgorithm: 'AES256' } },
        ],
      },
      LifecycleConfiguration: {
        Rules: Match.arrayWith([
          Match.objectLike({ ExpirationInDays: { Ref: 'RawRetentionDays' }, Status: 'Enabled' }),
        ]),
      },
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      },
    }));
    template.hasParameter('RawRetentionDays', {
      Type: 'Number',
      Default: 30,
      MinValue: 7,
      MaxValue: 365,
    });
  });

  test('grants bucket writes without object reads or deletes', () => {
    const statements = Object.values(template.findResources('AWS::IAM::Policy'))
      .flatMap(resource => resource.Properties.PolicyDocument.Statement)
      .filter(statement => JSON.stringify(statement.Resource).includes('NormalizedRawBucket'));
    expect(statements).toHaveLength(1);
    const actions = Array.isArray(statements[0].Action) ? statements[0].Action : [statements[0].Action];
    expect(actions).toContain('s3:PutObject');
    expect(actions).not.toContain('s3:GetObject*');
    expect(actions).not.toContain('s3:DeleteObject*');
  });

  test('limits registry-secret reads to the ECS execution role', () => {
    const secretPolicies = Object.values(template.findResources('AWS::IAM::Policy'))
      .filter(resource => JSON.stringify(resource.Properties.PolicyDocument.Statement)
        .includes('RegistryApiKeySecretArn'));
    expect(secretPolicies).toHaveLength(1);
    expect(JSON.stringify(secretPolicies[0].Properties.Roles)).toContain('ExecutionRole');
    expect(JSON.stringify(secretPolicies[0].Properties.Roles)).not.toContain('TaskRole');
  });

  test('alerts on gross monthly usage before credits are consumed', () => {
    template.hasParameter('BudgetAlertEmail', {
      Type: 'String',
      NoEcho: true,
    });
    template.hasResourceProperties('AWS::Budgets::Budget', {
      Budget: Match.objectLike({
        BudgetLimit: { Amount: 100, Unit: 'USD' },
        BudgetName: 'gnome-polymarket-sandbox',
        BudgetType: 'COST',
        CostTypes: Match.objectLike({
          IncludeCredit: false,
          IncludeRefund: false,
        }),
        TimeUnit: 'MONTHLY',
      }),
      NotificationsWithSubscribers: Match.arrayWith(
        [10, 25, 50, 100].map(threshold => Match.objectLike({
          Notification: Match.objectLike({
            NotificationType: 'ACTUAL',
            Threshold: threshold,
            ThresholdType: 'ABSOLUTE_VALUE',
          }),
        })),
      ),
    });
  });
});
