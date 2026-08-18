import { GnomeAccount, Stage } from "@gnome-trading-group/gnome-shared-cdk";

export const LAMBDAS_VERSION = "v1";

export interface MarketDataConfig {
  account: GnomeAccount;

  // Collector settings
  collectorOrchestratorVersion: string;
  registryApiKeyId: string;
}

const defaultConfig = {
  collectorOrchestratorVersion: "1.5.2",
}

const REGISTRY_API_KEY_IDS: { [stage in Stage]?: string } = {
  [Stage.DEV]: 'rb0pbivke8',
  [Stage.PROD]: 'mj0thnxe96',
};

export const CONFIGS: { [stage in Stage]?:  MarketDataConfig } = {
  [Stage.DEV]: {
    ...defaultConfig,
    account: GnomeAccount.InfraDev,
    registryApiKeyId: REGISTRY_API_KEY_IDS[Stage.DEV]!,
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
    registryApiKeyId: REGISTRY_API_KEY_IDS[Stage.PROD]!,
  },
}

export const GITHUB_REPO = "gnome-trading-group/gnome-market-data";
export const GITHUB_BRANCH = "release";
