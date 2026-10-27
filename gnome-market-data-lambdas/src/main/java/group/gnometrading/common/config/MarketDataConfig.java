package group.gnometrading.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.util.Map;

/**
 * The {@code market-data} Service Config, as JSON with snake_case keys. {@link #DEFAULTS} is sent as the defaults
 * header, so a key added here appears in the stored config on the next read; keys the config has that this doesn't
 * are ignored.
 *
 * @param rollToVersion the orchestrator release every shard moves to; null keeps shards where they are, and new shards
 *     take the latest release
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.ALWAYS)
public record MarketDataConfig(
        int maxStreamsPerShard,
        int retireGraceMinutes,
        Map<String, Integer> endedRetireGraceMinutes,
        String rollToVersion,
        int handoffMinutes,
        int emptyShardMinutes,
        Alerts alerts,
        QualityChecks qualityChecks) {

    /** How long a condition lasts before the reconciler alerts on it. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Alerts(int notLiveMinutes, int staleMinutes, int shardDownMinutes) {}

    /**
     * A second switch for quality checks on each final minute. They have no trigger today (see StorageStack), so this
     * only matters once that wiring is restored.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record QualityChecks(boolean enabled) {}

    public static final MarketDataConfig DEFAULTS = new MarketDataConfig(
            900,
            5,
            // Hyperliquid drops a connection holding both sides of an expired outcome market: retire those at once.
            Map.of("HYPERLIQUID", 0),
            null,
            2,
            10,
            new Alerts(10, 5, 5),
            new QualityChecks(true));
}
