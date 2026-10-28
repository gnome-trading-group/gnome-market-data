package group.gnometrading.collection.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.collection.model.Settings;
import group.gnometrading.common.config.MarketDataConfig;
import group.gnometrading.common.config.MarketDataServiceConfig;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ServiceConfigSettingsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MarketDataServiceConfig serviceConfig = new MarketDataServiceConfig(
            HttpClient.newHttpClient(), MAPPER, "http://127.0.0.1:9/", () -> "key", () -> 0);

    @Test
    void theDefaultsAreThePlansDecisions() {
        Settings settings = ServiceConfigSettings.of(MarketDataConfig.DEFAULTS, () -> "1.23.0");

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
        Settings settings = ServiceConfigSettings.of(config, () -> "1.23.0");

        assertEquals("1.24.0", settings.newShardVersion());
        assertEquals("1.24.0", settings.rollToVersion());
        assertEquals(400, settings.maxStreamsPerShard());
        assertEquals(TimeUnit.MINUTES.toNanos(2), settings.staleAlertNanos());
        assertEquals(
                TimeUnit.MINUTES.toNanos(10), settings.notLiveAlertNanos(), "the rest of alerts keeps its defaults");
        assertEquals(Map.of("HYPERLIQUID", 0), settings.endedRetireGraceMinutes());
    }

    @Test
    void anUnreachableServiceConfigFallsBackToTheDefaults() {
        assertEquals(MarketDataConfig.DEFAULTS, serviceConfig.get());
    }
}
