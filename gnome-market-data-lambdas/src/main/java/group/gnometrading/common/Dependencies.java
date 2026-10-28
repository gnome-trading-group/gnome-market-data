package group.gnometrading.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.RegistryConnection;
import group.gnometrading.SecurityMaster;
import group.gnometrading.collection.aws.DynamoStore;
import group.gnometrading.common.config.MarketDataServiceConfig;
import group.gnometrading.constants.Stage;
import group.gnometrading.inventory.CoverageRecord;
import group.gnometrading.resources.Properties;
import java.net.http.HttpClient;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.ssm.SsmClient;

/**
 * Everything a Lambda wires from its environment, built once per execution environment and only when first asked
 * for: each function reads only the env vars its stack gives it, and a client it never uses is never created.
 */
public final class Dependencies {

    private static final Dependencies INSTANCE = new Dependencies();

    private final Map<String, Object> built = new ConcurrentHashMap<>();
    private final Map<String, EcsClient> ecsClients = new ConcurrentHashMap<>();

    private Dependencies() {}

    public static Dependencies getInstance() {
        return INSTANCE;
    }

    @SuppressWarnings("unchecked")
    private <T> T once(final String key, final Supplier<T> build) {
        return (T) built.computeIfAbsent(key, ignored -> build.get());
    }

    private static String env(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is not set for this function");
        }
        return value;
    }

    // ----- shared -----

    public ObjectMapper getObjectMapper() {
        return once("objectMapper", ObjectMapper::new);
    }

    public Clock getClock() {
        return once("clock", Clock::systemUTC);
    }

    public S3Client getS3Client() {
        return once("s3", S3Client::create);
    }

    public DynamoDbClient getDynamoDbClient() {
        return once("dynamo", DynamoDbClient::create);
    }

    public DynamoDbEnhancedClient getDynamoDbEnhancedClient() {
        return once("dynamoEnhanced", () -> DynamoDbEnhancedClient.builder()
                .dynamoDbClient(getDynamoDbClient())
                .build());
    }

    /** The registry's security master, signed in with the key {@code REGISTRY_API_KEY_ID} names. */
    public SecurityMaster getSecurityMaster() {
        return once("securityMaster", () -> {
            final Stage stage = Stage.fromStageName(env("STAGE"));
            final Properties properties;
            try {
                properties = new Properties("market-data.%s.properties".formatted(stage.getStageName()));
            } catch (Exception error) {
                throw new IllegalStateException(error);
            }
            return new SecurityMaster(new RegistryConnection(
                    properties.getStringProperty("registry.url"), apiKey(env("REGISTRY_API_KEY_ID"))));
        });
    }

    /** The market-data Service Config, from the controller ({@code CONTROLLER_API_URL}, {@code CONTROLLER_API_KEY_ID}). */
    public MarketDataServiceConfig getServiceConfig() {
        return once("serviceConfig", () -> {
            final String keyId = env("CONTROLLER_API_KEY_ID");
            return new MarketDataServiceConfig(
                    HttpClient.newHttpClient(),
                    getObjectMapper(),
                    env("CONTROLLER_API_URL"),
                    () -> apiKey(keyId),
                    System::nanoTime);
        });
    }

    /** An API Gateway key's value; keys live in us-east-1 whatever region the function runs in. */
    private String apiKey(final String keyId) {
        final ApiGatewayClient apiGateway = once(
                "apiGateway",
                () -> ApiGatewayClient.builder().region(Region.US_EAST_1).build());
        return apiGateway
                .getApiKey(request -> request.apiKey(keyId).includeValue(true))
                .value();
    }

    // ----- buckets and tables -----

    public String getMetadataBucketName() {
        return env("METADATA_BUCKET_NAME");
    }

    public DynamoDbTable<CoverageRecord> getCoverageTable() {
        return once("coverageTable", () -> getDynamoDbEnhancedClient()
                .table(env("COVERAGE_TABLE_NAME"), TableSchema.fromBean(CoverageRecord.class)));
    }

    // ----- collection -----

    public DynamoStore.Tables getCollectionTables() {
        return once(
                "collectionTables",
                () -> new DynamoStore.Tables(
                        env("COLLECTION_RULES_TABLE"),
                        env("COLLECTION_TARGETS_TABLE"),
                        env("COLLECTION_SHARDS_TABLE"),
                        env("COLLECTION_COVERAGE_TABLE"),
                        env("COLLECTION_GAPS_TABLE")));
    }

    /** ECS in {@code region}: shards run in every collection region, the reconciler only in us-east-1. */
    public EcsClient getEcsClient(final String region) {
        return ecsClients.computeIfAbsent(
                region, ignored -> EcsClient.builder().region(Region.of(region)).build());
    }

    public EventBridgeClient getEventBridgeClient() {
        return once("eventBridge", EventBridgeClient::create);
    }

    public SsmClient getSsmClient() {
        return once("ssm", SsmClient::create);
    }
}
