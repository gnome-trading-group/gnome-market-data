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
        Map<String, ShardCaps> shardCaps,
        double defaultStreamRecordsPerSecond,
        int retireGraceMinutes,
        Map<String, Integer> endedRetireGraceMinutes,
        String rollToVersion,
        int handoffMinutes,
        int emptyShardMinutes,
        Alerts alerts,
        Consolidation consolidation,
        Rebalance rebalance,
        List<PartitionShards> minShards,
        List<PartitionShards> maxShards) {

    /**
     * What one shard of a tier ({@code standard}, {@code low_latency}) may carry: collected streams, and its load,
     * published (Aeron) streams and collected records per second counted together ({@code published / max_published +
     * records_per_second / max_records_per_second}, at most 1). Placement never exceeds either; a shard over its load is
     * relieved and alerted on.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ShardCaps(int maxStreams, int maxPublished, double maxRecordsPerSecond) {}

    /** How long a condition lasts before the reconciler alerts on it. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Alerts(
            int notLiveMinutes,
            int staleMinutes,
            int shardDownMinutes,
            int leaseOnEndedStreamMinutes,
            int overCapacityMinutes,
            int homeZoneUnplacedMinutes) {}

    /**
     * Merging a partition's under-filled shards. A shard whose streams would all fit on the partition's others, each
     * left at most {@code maxFill} of {@code max_streams_per_shard}, for {@code afterMinutes} running drains into
     * them; {@code dryRun} only logs and records it.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Consolidation(boolean enabled, boolean dryRun, int afterMinutes, double maxFill) {}

    /**
     * Relieving a shard over its published or records-per-second cap for {@code relieveAfterMinutes}: streams move
     * make-before-break onto shards left at most {@code relieveTargetFill} full (or a new one within
     * {@code max_shards}) until it is at most that full too. A shard a merge or relief touched is left alone for
     * {@code shardCooldownMinutes}, a stream one moved for {@code streamCooldownMinutes}; at most
     * {@code maxMovesPerTick} streams move a tick, {@code maxLeasedMovesPerTick} of them leased. {@code dryRun} only
     * logs the moves.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Rebalance(
            boolean enabled,
            boolean dryRun,
            int relieveAfterMinutes,
            double relieveTargetFill,
            int shardCooldownMinutes,
            int streamCooldownMinutes,
            int maxMovesPerTick,
            int maxLeasedMovesPerTick) {}

    /**
     * A shard count for one partition (region, source kind, priority, tier; tier defaults to {@code standard}), as
     * {@code min_shards} and {@code max_shards} list them. {@code min_shards}: {@code count} ACTIVE shards the partition
     * keeps even when empty, so a session lease there skips the 3-5 minute cold start. {@code max_shards}: the most
     * shards the partition runs; streams that would need another stay unplaced. A version roll (or a merge) may run one
     * shard over it while it moves streams. Partitions not listed keep none and have no cap; where both are set and min
     * exceeds max, max wins. {@code zoneId} names a production partition's home zone (its exchange's): an entry applies
     * to exactly the partition it names, so a production partition held to a zone needs its zone here, and one without
     * a zone covers only zone-free partitions (research, and venues with no home zone). Ignored on research entries.
     */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PartitionShards(
            String region, String sourceKind, String priority, String tier, int count, String zoneId) {}

    public static final MarketDataConfig DEFAULTS = new MarketDataConfig(
            // Each below the load at which the tier's latency targets failed in load tests: standard (r7a.medium)
            // failed near 550 published streams, low_latency (r7a.large) near 450. Standard passed 5.6k records/s with
            // 30 published, but 250 published at 2.8k failed and nothing between was tested, so its rate cap stays near
            // that mix rather than the light-publishing limit. Collected streams lengthen the tail on one vCPU even
            // when
            // quiet.
            Map.of(
                    "standard", new ShardCaps(900, 300, 4_500),
                    "low_latency", new ShardCaps(900, 300, 50_000)),
            // Typical streams run well under one record a second; this keeps 900 unread streams under the rate cap.
            1.0,
            5,
            // Hyperliquid drops a connection holding both sides of an expired outcome market: retire those at once.
            Map.of("HYPERLIQUID", 0),
            null,
            2,
            10,
            // A session normally exits within minutes of its market closing; an hour on means it is stuck.
            // A production shard held to its venue's zone waits 5 minutes for a host there before it is short.
            new Alerts(10, 5, 5, 60, 10, 5),
            // Fifteen minutes is three rule resolutions plus a retire grace: long enough that a dip has settled.
            new Consolidation(true, false, 15, 0.8),
            // Relief starts above a cap and stops at 0.8 of it, the fill merges leave, so neither undoes the other.
            new Rebalance(true, true, 10, 0.8, 30, 60, 25, 3),
            // No warm shards until cold starts prove too slow for launches.
            List.of(),
            List.of());
}
