package group.gnometrading.collection.model;

import java.util.Map;

/**
 * Runtime settings, from Service Config ({@code market-data}). New shards run {@code newShardVersion}; existing
 * ones roll only when {@code rollToVersion} is set, so an orchestrator release never moves running shards by itself.
 *
 * @param minShards ACTIVE shards each partition keeps even when empty, so a lease on a new stream skips the cold start;
 *     a partition not listed keeps none
 * @param maxShards the most shards each listed partition runs (a roll or merge may go one over while it moves
 *     streams); a partition not listed has no cap
 * @param shardCaps each tier's shard caps; a tier not listed takes {@link #STANDARD_TIER}'s, which is always there
 * @param defaultStreamRecordsPerSecond the rate placement counts for a stream with no reading, when no stream of its
 *     source has one either
 */
public record Settings(
        Map<String, ShardCaps> shardCaps,
        double defaultStreamRecordsPerSecond,
        int defaultRetireGraceMinutes,
        Map<String, Integer> endedRetireGraceMinutes,
        String newShardVersion,
        String rollToVersion,
        long handoffNanos,
        long emptyShardNanos,
        long notLiveAlertNanos,
        long staleAlertNanos,
        long shardDownNanos,
        long leaseOnEndedStreamAlertNanos,
        long overCapacityAlertNanos,
        long homeZoneUnplacedNanos,
        boolean consolidationEnabled,
        boolean consolidationDryRun,
        long consolidateAfterNanos,
        double consolidationMaxFill,
        Map<Partition, Integer> minShards,
        Map<Partition, Integer> maxShards,
        Rebalance rebalance) {

    public static final String STANDARD_TIER = "standard";

    /**
     * Relieving shards over a cap. One over a cap for {@code relieveAfterNanos} moves streams make-before-break onto
     * shards left at most {@code relieveTargetFill} of their caps, until it is at most that full itself; {@code dryRun}
     * only logs the moves. A shard a merge or relief touched is left alone for {@code shardCooldownNanos}, and a stream
     * one moved for {@code streamCooldownNanos}. At most {@code maxMovesPerTick} streams move a tick, at most
     * {@code maxLeasedMovesPerTick} of them leased.
     */
    public record Rebalance(
            boolean enabled,
            boolean dryRun,
            long relieveAfterNanos,
            double relieveTargetFill,
            long shardCooldownNanos,
            long streamCooldownNanos,
            int maxMovesPerTick,
            int maxLeasedMovesPerTick) {}

    public ShardCaps caps(final Partition partition) {
        final ShardCaps caps = shardCaps.get(partition.tier());
        return caps != null ? caps : shardCaps.get(STANDARD_TIER);
    }

    public int maxShards(final Partition partition) {
        return maxShards.getOrDefault(partition, Integer.MAX_VALUE);
    }
}
