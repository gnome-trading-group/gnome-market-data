package group.gnometrading.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.util.List;
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
        Consolidation consolidation,
        List<PartitionShards> minShards,
        List<PartitionShards> maxShards) {

    /** How long a condition lasts before the reconciler alerts on it. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Alerts(int notLiveMinutes, int staleMinutes, int shardDownMinutes) {}

    /**
     * Merging a partition's under-filled shards. A shard whose streams would all fit on the partition's others, each
     * left at most {@code maxFill} of {@code max_streams_per_shard}, for {@code afterMinutes} running drains into
     * them; {@code dryRun} only logs and records it.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Consolidation(boolean enabled, boolean dryRun, int afterMinutes, double maxFill) {}

    /**
     * A shard count for one partition (region, source kind, priority, tier; tier defaults to {@code standard}), as
     * {@code min_shards} and {@code max_shards} list them. {@code min_shards}: {@code count} ACTIVE shards the partition
     * keeps even when empty, so a session lease there skips the 3-5 minute cold start. {@code max_shards}: the most
     * shards the partition runs; streams that would need another stay unplaced. A version roll (or a merge) may run one
     * shard over it while it moves streams. Partitions not listed keep none and have no cap; where both are set and min
     * exceeds max, max wins.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PartitionShards(String region, String sourceKind, String priority, String tier, int count) {}

    public static final MarketDataConfig DEFAULTS = new MarketDataConfig(
            900,
            5,
            // Hyperliquid drops a connection holding both sides of an expired outcome market: retire those at once.
            Map.of("HYPERLIQUID", 0),
            null,
            2,
            10,
            new Alerts(10, 5, 5),
            // Fifteen minutes is three rule resolutions plus a retire grace: long enough that a dip has settled.
            new Consolidation(true, false, 15, 0.8),
            // No warm shards until cold starts prove too slow for launches.
            List.of(),
            List.of());
}
