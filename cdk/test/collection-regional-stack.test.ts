import * as cdk from "aws-cdk-lib";
import { Annotations, Match, Template } from "aws-cdk-lib/assertions";
import { Stage } from "@gnome-trading-group/gnome-shared-cdk";
import { CollectionRegionalStack } from "../lib/stacks/collection-regional-stack";
import { CONFIGS } from "../lib/config";

// The platform network's lookup as the account would answer it: a public subnet in each of eu-west-1's platform zones.
const VPC_KEY = "vpc-provider:account=123456789012:filter.tag:gnome:network=platform:region=eu-west-1:returnAsymmetricSubnets=true";
const PLATFORM_CONTEXT = {
  [VPC_KEY]: {
    vpcId: "vpc-0platform", vpcCidrBlock: "10.144.0.0/16", ownerAccountId: "123456789012", availabilityZones: [],
    subnetGroups: [{ name: "public", type: "Public", subnets: [
      { subnetId: "subnet-a", cidr: "10.144.0.0/20", availabilityZone: "eu-west-1a", routeTableId: "rtb-a" },
      { subnetId: "subnet-b", cidr: "10.144.16.0/20", availabilityZone: "eu-west-1b", routeTableId: "rtb-b" },
    ] }],
  },
  "security-group:account=123456789012:region=eu-west-1:securityGroupName=gnome-platform-sg:vpcId=vpc-0platform": {
    securityGroupId: "sg-0platform", allowAllOutbound: false,
  },
};

const PROPS = {
  env: { account: "123456789012", region: "eu-west-1" },
  config: CONFIGS[Stage.DEV]!,
  deploymentRegion: "eu-west-1",
  finalBucketName: "final",
  exchangeRawBucketName: "raw",
  metadataBucketName: "metadata",
  registryApiKeyId: "key-id",
};

describe("the collection regional stack", () => {
  const app = new cdk.App({ context: PLATFORM_CONTEXT });
  const stack = new CollectionRegionalStack(app, "Collection", PROPS);
  const template = Template.fromStack(stack);
  const launchTemplates = Object.values(template.findResources("AWS::EC2::LaunchTemplate")) as any[];
  const byType = (type: string) => launchTemplates.find((t) => t.Properties.LaunchTemplateData.InstanceType === type);

  it("launches on-demand (production) hosts into the precision-time group, and Spot hosts outside it", () => {
    // The region's on-demand, each zone's on-demand, and Spot.
    expect(launchTemplates).toHaveLength(4);
    const inGroup = launchTemplates.filter((t) => t.Properties.LaunchTemplateData.Placement);
    expect(inGroup).toHaveLength(3);
    for (const onDemand of inGroup) {
      expect(onDemand.Properties.LaunchTemplateData.Placement.GroupName).toEqual(
        expect.objectContaining({ Ref: expect.stringMatching(/precisiontimeplacementgroup/i) }));
    }
    expect(byType("r7a.medium")).toBeDefined();
  });

  it("has on-demand hosts per zone for production shards held to a home zone, trying the next type in the zone", () => {
    const groups = template.findResources("AWS::AutoScaling::AutoScalingGroup");
    const providers = Object.values(template.findResources("AWS::ECS::CapacityProvider")) as any[];
    for (const [zoneId, subnet] of [["euw1-az2", "subnet-a"], ["euw1-az3", "subnet-b"]]) {
      const provider = providers.find((p) => p.Properties.Name === `collection-ondemand-eu-west-1-${zoneId}`);
      expect(provider).toBeDefined();
      const group = groups[provider.Properties.AutoScalingGroupProvider.AutoScalingGroupArn.Ref];
      expect(group.Properties.VPCZoneIdentifier).toEqual([subnet]);
      expect(group.Properties.MixedInstancesPolicy.LaunchTemplate.Overrides.map((o: any) => o.InstanceType))
        .toEqual(["r7a.medium", "m7a.large", "m7i.large"]);
      expect(group.Properties.MinSize).toBe("0");
    }
    const spot = groups[providers.find((p) => p.Properties.Name === "collection-spot-eu-west-1")
      .Properties.AutoScalingGroupProvider.AutoScalingGroupArn.Ref];
    expect(spot.Properties.VPCZoneIdentifier).toEqual(["subnet-a", "subnet-b"]);
  });

  it("reserves the health port on its host, so ECS never places two shards on one host", () => {
    const [task] = Object.values(template.findResources("AWS::ECS::TaskDefinition")) as any[];
    expect(task.Properties.ContainerDefinitions[0].PortMappings)
      .toEqual([{ ContainerPort: 8080, HostPort: 8080, Protocol: "tcp" }]);
  });

  it("refuses a zone the platform network has no subnet in", () => {
    const oneZone = new cdk.App({ context: {
      ...PLATFORM_CONTEXT,
      [VPC_KEY]: { ...PLATFORM_CONTEXT[VPC_KEY], subnetGroups: [{ name: "public", type: "Public", subnets: [
        { subnetId: "subnet-a", cidr: "10.144.0.0/20", availabilityZone: "eu-west-1a", routeTableId: "rtb-a" },
      ] }] },
    } });
    const stack = new CollectionRegionalStack(oneZone, "Collection", PROPS);
    Annotations.fromStack(stack).hasError("*", Match.stringLikeRegexp("no public subnet in eu-west-1b"));
  });

  it("still builds against the placeholder network of an uncached lookup, so the lookup can run", () => {
    expect(() => new CollectionRegionalStack(new cdk.App(), "Collection",
      { ...PROPS, env: { account: "123456789012", region: "us-east-2" }, deploymentRegion: "us-east-2" })).not.toThrow();
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
