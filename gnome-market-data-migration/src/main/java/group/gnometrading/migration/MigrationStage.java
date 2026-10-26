package group.gnometrading.migration;

import group.gnometrading.RegistryConnection;
import group.gnometrading.SecurityMaster;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;

/** One stage's buckets and registry, as the pipeline Lambdas configure them (market-data.{stage}.properties). */
enum MigrationStage {
    DEV("dev", "i3116oczxe.execute-api.us-east-1.amazonaws.com", "rb0pbivke8"),
    PROD("prod", "n5dxpwnij0.execute-api.us-east-1.amazonaws.com", "mj0thnxe96");

    private final String name;
    private final String registryUrl;
    private final String registryApiKeyId;

    MigrationStage(final String name, final String registryUrl, final String registryApiKeyId) {
        this.name = name;
        this.registryUrl = registryUrl;
        this.registryApiKeyId = registryApiKeyId;
    }

    static MigrationStage of(final String name) {
        for (final MigrationStage stage : values()) {
            if (stage.name.equals(name)) {
                return stage;
            }
        }
        throw new IllegalArgumentException("Unknown stage " + name + "; expected dev or prod");
    }

    String mergedBucket() {
        return "gnome-market-data-merged-" + this.name;
    }

    String finalBucket() {
        return "gnome-market-data-" + this.name;
    }

    String rawBucket() {
        return "gnome-market-data-raw-" + this.name;
    }

    String journalsBucket() {
        return "gnome-journals-" + this.name;
    }

    SecurityMaster securityMaster() {
        try (ApiGatewayClient client =
                ApiGatewayClient.builder().region(Region.US_EAST_1).build()) {
            final String apiKey = client.getApiKey(
                            r -> r.apiKey(this.registryApiKeyId).includeValue(true))
                    .value();
            return new SecurityMaster(new RegistryConnection(this.registryUrl, apiKey));
        }
    }

    @Override
    public String toString() {
        return this.name;
    }
}
