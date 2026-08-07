#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { SandboxCollectorStack } from '../lib/sandbox-collector-stack';

const app = new cdk.App();
const account = process.env.CDK_DEFAULT_ACCOUNT;
if (!account) {
  throw new Error('CDK_DEFAULT_ACCOUNT is unavailable. Run CDK with --profile gnome-sandbox.');
}

new SandboxCollectorStack(app, 'GnomePolymarketSandbox', {
  env: {
    account,
    region: 'us-east-1',
  },
  description: 'Development-only, parameterized Polymarket raw-data collector.',
});

app.synth();
