package group.gnometrading.collection.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.Settings;
import group.gnometrading.common.config.MarketDataConfig;
import group.gnometrading.common.config.MarketDataServiceConfig;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ServiceConfigSettingsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Set<String> REGIONS = Set.of("us-east-1", "ap-northeast-1", "eu-west-1");
    private static final Partition US_PRODUCTION = new Partition("us-east-1", "VENUE", Priority.PRODUCTION, "standard");

    private final MarketDataServiceConfig serviceConfig = new MarketDataServiceConfig(
            HttpClient.newHttpClient(), MAPPER, "http://127.0.0.1:9/", () -> "key", () -> 0);

    @Test
    void theDefaultsAreThePlansDecisions() {
        Settings settings = ServiceConfigSettings.of(MarketDataConfig.DEFAULTS, () -> "1.23.0", REGIONS);

        assertEquals(900, settings.maxStreamsPerShard());
        assertEquals(5, settings.defaultRetireGraceMinutes());
        assertEquals(Map.of("HYPERLIQUID", 0), settings.endedRetireGraceMinutes());
        assertEquals("1.23.0", settings.newShardVersion(), "the latest release, with no roll asked for");
        assertNull(settings.rollToVersion());
        assertEquals(TimeUnit.MINUTES.toNanos(2), settings.handoffNanos());
        assertEquals(TimeUnit.MINUTES.toNanos(5), settings.shardDownNanos());
    }

    @Test
    void theDefaultsAreSentAsSnakeCaseJson() throws Exception {
        var json = MAPPER.valueToTree(MarketDataConfig.DEFAULTS);

        assertEquals(900, json.get("max_streams_per_shard").asInt());
        assertTrue(json.get("roll_to_version").isNull(), "present, so the stored config shows the key");
        assertEquals(10, json.get("alerts").get("not_live_minutes").asInt());
    }

    @Test
    void aStoredConfigOverridesOnlyWhatItSets() throws Exception {
        MarketDataConfig config =
                serviceConfig.parse(MAPPER.readTree("{\"roll_to_version\": \"1.24.0\", \"max_streams_per_shard\": 400,"
                        + " \"alerts\": {\"stale_minutes\": 2},"
                        + " \"something_removed\": 1}"));
        Settings settings = ServiceConfigSettings.of(config, () -> "1.23.0", REGIONS);

        assertEquals("1.24.0", settings.newShardVersion());
        assertEquals("1.24.0", settings.rollToVersion());
        assertEquals(400, settings.maxStreamsPerShard());
        assertEquals(TimeUnit.MINUTES.toNanos(2), settings.staleAlertNanos());
        assertEquals(
                TimeUnit.MINUTES.toNanos(10), settings.notLiveAlertNanos(), "the rest of alerts keeps its defaults");
        assertEquals(Map.of("HYPERLIQUID", 0), settings.endedRetireGraceMinutes());
    }

    @Test
    void minShardsIsEmptyByDefaultAndKeyedByPartition() throws Exception {
        assertEquals(
                Map.of(),
                ServiceConfigSettings.of(MarketDataConfig.DEFAULTS, () -> "1.23.0", REGIONS)
                        .minShards());

        MarketDataConfig config = serviceConfig.parse(MAPPER.readTree("{\"min_shards\": ["
                + "{\"region\": \"us-east-1\", \"source_kind\": \"venue\", \"priority\": \"PRODUCTION\","
                + " \"count\": 1},"
                + "{\"region\": \"eu-west-1\", \"source_kind\": \"VENUE\", \"priority\": \"URGENT\","
                + " \"tier\": \"standard\", \"count\": 2}]}"));

        assertEquals(
                Map.of(new Partition("us-east-1", "VENUE", Priority.PRODUCTION, "standard"), 1),
                ServiceConfigSettings.of(config, () -> "1.23.0", REGIONS).minShards(),
                "tier defaults to standard; an unknown priority is skipped, not fatal");
    }

    @Test
    void malformedShardCountsAndOnesOutsideTheCollectionRegionsAreSkipped() throws Exception {
        MarketDataConfig config = serviceConfig.parse(MAPPER.readTree("{\"min_shards\": ["
                + "{\"region\": \"us-east-1\", \"source_kind\": \"VENUE\", \"priority\": \"PRODUCTION\","
                + " \"count\": 1},"
                + "{\"region\": \"us-west-2\", \"source_kind\": \"VENUE\", \"priority\": \"PRODUCTION\","
                + " \"count\": 1},"
                + "{\"region\": \"us-east-1\", \"priority\": \"RESEARCH\", \"count\": 1},"
                + "{\"source_kind\": \"VENUE\", \"priority\": \"RESEARCH\", \"count\": 1}],"
                + " \"max_shards\": ["
                + "{\"region\": \"mars-1\", \"source_kind\": \"VENUE\", \"priority\": \"RESEARCH\","
                + " \"count\": 1},"
                + "{\"region\": \"us-east-1\", \"source_kind\": \"VENUE\", \"priority\": \"RESEARCH\","
                + " \"count\": -1}]}"));

        Settings settings = ServiceConfigSettings.of(config, () -> "1.23.0", REGIONS);

        assertEquals(Map.of(US_PRODUCTION, 1), settings.minShards());
        assertEquals(Map.of(), settings.maxShards());
    }

    @Test
    void maxShardsIsEmptyByDefaultAndKeyedByPartitionWithZeroAllowed() throws Exception {
        assertEquals(
                Map.of(),
                ServiceConfigSettings.of(MarketDataConfig.DEFAULTS, () -> "1.23.0", REGIONS)
                        .maxShards());

        MarketDataConfig config = serviceConfig.parse(MAPPER.readTree("{\"max_shards\": ["
                + "{\"region\": \"us-east-1\", \"source_kind\": \"venue\", \"priority\": \"production\","
                + " \"count\": 4},"
                + "{\"region\": \"us-east-1\", \"source_kind\": \"VENUE\", \"priority\": \"PRODUCTION\","
                + " \"count\": 3},"
                + "{\"region\": \"eu-west-1\", \"source_kind\": \"VENUE\", \"priority\": \"RESEARCH\","
                + " \"count\": 0}]}"));
        Settings settings = ServiceConfigSettings.of(config, () -> "1.23.0", REGIONS);

        assertEquals(
                Map.of(US_PRODUCTION, 3, new Partition("eu-west-1", "VENUE", Priority.RESEARCH, "standard"), 0),
                settings.maxShards(),
                "listed twice, the lower cap holds");
        assertEquals(3, settings.maxShards(US_PRODUCTION));
        assertEquals(
                Integer.MAX_VALUE,
                settings.maxShards(new Partition("us-east-1", "VENUE", Priority.RESEARCH, "standard")),
                "a partition not listed has no cap");
    }

    @Test
    void aMinShardsAboveMaxShardsIsLoweredToIt() throws Exception {
        MarketDataConfig config = serviceConfig.parse(MAPPER.readTree("{"
                + "\"min_shards\": [{\"region\": \"us-east-1\", \"source_kind\": \"VENUE\","
                + " \"priority\": \"PRODUCTION\", \"count\": 3}],"
                + "\"max_shards\": [{\"region\": \"us-east-1\", \"source_kind\": \"VENUE\","
                + " \"priority\": \"PRODUCTION\", \"count\": 2}]}"));

        Settings settings = ServiceConfigSettings.of(config, () -> "1.23.0", REGIONS);

        assertEquals(Map.of(US_PRODUCTION, 2), settings.minShards());
        assertEquals(Map.of(US_PRODUCTION, 2), settings.maxShards());
    }

    @Test
    void anUnreachableServiceConfigFallsBackToTheDefaults() {
        assertEquals(MarketDataConfig.DEFAULTS, serviceConfig.get());
    }
}
