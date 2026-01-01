#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { GnomeAccount } from '@gnome-trading-group/gnome-shared-cdk';
import { MarketDataPipelineStack } from '../lib/market-data-pipeline-stack';

const app = new cdk.App();
new MarketDataPipelineStack(app, 'MarketDataPipelineStack', {
  env: GnomeAccount.InfraPipelines.environment,
});
app.synth();
