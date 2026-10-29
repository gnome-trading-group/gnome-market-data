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
 */
public record Settings(
        int maxStreamsPerShard,
        int defaultRetireGraceMinutes,
        Map<String, Integer> endedRetireGraceMinutes,
        String newShardVersion,
        String rollToVersion,
        long handoffNanos,
        long emptyShardNanos,
        long notLiveAlertNanos,
        long staleAlertNanos,
        long shardDownNanos,
        boolean consolidationEnabled,
        boolean consolidationDryRun,
        long consolidateAfterNanos,
        double consolidationMaxFill,
        Map<Partition, Integer> minShards,
        Map<Partition, Integer> maxShards) {

    public int maxShards(final Partition partition) {
        return maxShards.getOrDefault(partition, Integer.MAX_VALUE);
    }
}
