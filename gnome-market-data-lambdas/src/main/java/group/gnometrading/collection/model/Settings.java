package group.gnometrading.collection.model;

import java.util.Map;

/**
 * Runtime settings, from Service Config ({@code market-data}). New shards run {@code newShardVersion}; existing
 * ones roll only when {@code rollToVersion} is set, so an orchestrator release never moves running shards by itself.
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
        double consolidationMaxFill) {}
