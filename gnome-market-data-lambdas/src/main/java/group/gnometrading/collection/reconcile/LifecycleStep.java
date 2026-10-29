package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.ShardRecord;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Shard lifecycle: one shard at a time rolls to the configured version (it drains, its streams hand off to a shard on
 * the new version); a shard empty long enough drains; a drained shard is deleted. A partition with {@code min_shards}
 * keeps that many ACTIVE shards that take streams, starting empty ones to make up the number, so a lease there finds a
 * running shard instead of waiting minutes for a host. It never starts one that would take the partition past its
 * {@code max_shards}, counting every shard it runs, draining ones too: mid roll the top-up waits for the old shards to
 * go.
 */
final class LifecycleStep {

    private final Function<Partition, String> newShardId;

    LifecycleStep(final Function<Partition, String> newShardId) {
        this.newShardId = newShardId;
    }

    void rollAndScaleDown(final Tick tick) {
        roll(tick);
        final Map<Partition, Integer> serving = serving(tick);
        for (ShardRecord shard : List.copyOf(tick.shards.values())) {
            if (shard.state() != ShardRecord.State.ACTIVE) {
                continue;
            }
            if (!shard.isEmpty()) {
                tick.shards.put(shard.shardId(), shard.withEmptySince(0));
            } else if (shard.emptySinceNanos() == 0) {
                tick.shards.put(shard.shardId(), shard.withEmptySince(tick.now));
            } else if (tick.now - shard.emptySinceNanos() >= tick.config.emptyShardNanos()) {
                scaleDown(tick, shard, serving);
            }
        }
        topUp(tick, serving);
    }

    private static void roll(final Tick tick) {
        final String rollTo = tick.config.rollToVersion();
        final boolean rolling = tick.shards.values().stream()
                .anyMatch(shard -> shard.state() == ShardRecord.State.DRAINING && !shard.isEmpty());
        if (!rolling && rollTo != null) {
            tick.shards.values().stream()
                    .filter(shard -> shard.state() == ShardRecord.State.ACTIVE)
                    .filter(shard -> !rollTo.equals(shard.version()) && !shard.isEmpty())
                    .min(Comparator.comparingLong(ShardRecord::createdAtNanos))
                    .ifPresent(shard -> tick.shards.put(shard.shardId(), shard.withState(ShardRecord.State.DRAINING)));
        }
    }

    /** Drains an empty shard unless its partition would drop below {@code min_shards}. */
    private static void scaleDown(final Tick tick, final ShardRecord shard, final Map<Partition, Integer> serving) {
        final boolean counted = PlacementStep.takesStreams(tick, shard);
        if (counted && serving.getOrDefault(shard.partition(), 0) <= minShards(tick, shard.partition())) {
            return;
        }
        tick.shards.put(shard.shardId(), shard.withState(ShardRecord.State.DRAINING));
        if (counted) {
            serving.merge(shard.partition(), -1, Integer::sum);
        }
    }

    void deleteDrained(final Tick tick) {
        for (ShardRecord shard : List.copyOf(tick.shards.values())) {
            if (shard.state() == ShardRecord.State.DRAINING && shard.isEmpty()) {
                tick.shards.put(shard.shardId(), shard.withState(ShardRecord.State.DELETED));
            }
        }
    }

    /** Whether draining {@code shard} would leave its partition below {@code min_shards}. */
    static boolean atMinimum(final Tick tick, final ShardRecord shard) {
        final int min = minShards(tick, shard.partition());
        return min > 0
                && PlacementStep.takesStreams(tick, shard)
                && serving(tick).getOrDefault(shard.partition(), 0) <= min;
    }

    private void topUp(final Tick tick, final Map<Partition, Integer> serving) {
        final Map<Partition, Integer> running = new HashMap<>();
        for (ShardRecord shard : tick.shards.values()) {
            if (shard.state() != ShardRecord.State.DELETED) {
                running.merge(shard.partition(), 1, Integer::sum);
            }
        }
        tick.config.minShards().forEach((partition, min) -> {
            final int room = tick.config.maxShards(partition) - running.getOrDefault(partition, 0);
            final int missing = min - serving.getOrDefault(partition, 0);
            for (int count = 0; count < Math.min(missing, room); count++) {
                final ShardRecord created = ShardRecord.created(
                        newShardId.apply(partition), partition, tick.config.newShardVersion(), tick.now);
                tick.shards.put(created.shardId(), created);
            }
        });
    }

    /** Per partition, the ACTIVE shards that take streams: during a roll, only those on the new version. */
    private static Map<Partition, Integer> serving(final Tick tick) {
        final Map<Partition, Integer> serving = new HashMap<>();
        for (ShardRecord shard : tick.shards.values()) {
            if (PlacementStep.takesStreams(tick, shard)) {
                serving.merge(shard.partition(), 1, Integer::sum);
            }
        }
        return serving;
    }

    private static int minShards(final Tick tick, final Partition partition) {
        return tick.config.minShards().getOrDefault(partition, 0);
    }
}
