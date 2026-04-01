import { GnomeAccount, Stage } from "@gnome-trading-group/gnome-shared-cdk";

export const LAMBDAS_VERSION = "v1";

export interface MarketDataConfig {
  account: GnomeAccount;

  // Collector settings
  collectorOrchestratorVersion: string;
}

const defaultConfig = {
  collectorOrchestratorVersion: "1.1.28",
}

export const CONFIGS: { [stage in Stage]?:  MarketDataConfig } = {
  [Stage.DEV]: {
    ...defaultConfig,
    account: GnomeAccount.InfraDev,
  },
  // [Stage.STAGING]: {
  //   ...defaultConfig,
  //   account: GnomeAccount.InfraStaging,

  //   slackChannelConfigurationName: "gnome-alerts-staging",
  //   slackChannelId: "C08KL9PGAQZ",
  // }, 
  [Stage.PROD]: {
    ...defaultConfig,
    account: GnomeAccount.InfraProd,
  },
}

export const GITHUB_REPO = "gnome-trading-group/gnome-market-data";
export const GITHUB_BRANCH = "release";
