import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import { Stage } from "@gnome-trading-group/gnome-shared-cdk";
import { CollectionRegionalStack } from "../lib/stacks/collection-regional-stack";
import { CONFIGS } from "../lib/config";

describe("the collection regional stack", () => {
  const app = new cdk.App();
  const stack = new CollectionRegionalStack(app, "Collection", {
    env: { account: "123456789012", region: "eu-west-1" },
    config: CONFIGS[Stage.DEV]!,
    deploymentRegion: "eu-west-1",
    finalBucketName: "final",
    exchangeRawBucketName: "raw",
    metadataBucketName: "metadata",
    registryApiKeyId: "key-id",
  });
  const template = Template.fromStack(stack);
  const launchTemplates = Object.values(template.findResources("AWS::EC2::LaunchTemplate")) as any[];
  const byType = (type: string) => launchTemplates.find((t) => t.Properties.LaunchTemplateData.InstanceType === type);

  it("launches on-demand (production) hosts into the precision-time group, and Spot hosts outside it", () => {
    expect(launchTemplates).toHaveLength(2);
    const [onDemand, spot] = [launchTemplates[0], launchTemplates[1]].sort((a, b) =>
      (a.Properties.LaunchTemplateData.Placement ? 0 : 1) - (b.Properties.LaunchTemplateData.Placement ? 0 : 1));
    expect(onDemand.Properties.LaunchTemplateData.Placement.GroupName).toEqual(
      expect.objectContaining({ Ref: expect.stringMatching(/precisiontimeplacementgroup/i) }));
    expect(spot.Properties.LaunchTemplateData.Placement).toBeUndefined();
    expect(byType("r7a.medium")).toBeDefined();
  });

  it("sets up and reports the clock on every host, and shows its state to the shard read-only", () => {
    for (const lt of launchTemplates) {
      expect(JSON.stringify(lt.Properties.LaunchTemplateData.UserData)).toContain("gnome-clock-setup.service");
    }
    const [task] = Object.values(template.findResources("AWS::ECS::TaskDefinition")) as any[];
    expect(task.Properties.Volumes).toContainEqual({ Name: "clock", Host: { SourcePath: "/run/gnome" } });
    expect(task.Properties.ContainerDefinitions[0].MountPoints).toContainEqual(
      { SourceVolume: "clock", ContainerPath: "/run/gnome", ReadOnly: true });
  });
});
