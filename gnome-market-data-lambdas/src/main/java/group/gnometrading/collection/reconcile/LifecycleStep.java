package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.ShardRecord;
import java.util.Comparator;
import java.util.List;

/**
 * Shard lifecycle: one shard at a time rolls to the configured version (it drains, its streams hand off to a shard on
 * the new version); a shard empty long enough drains; a drained shard is deleted.
 */
final class LifecycleStep {

    void rollAndScaleDown(final Tick tick) {
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
        for (ShardRecord shard : List.copyOf(tick.shards.values())) {
            if (shard.state() != ShardRecord.State.ACTIVE) {
                continue;
            }
            if (!shard.isEmpty()) {
                tick.shards.put(shard.shardId(), shard.withEmptySince(0));
            } else if (shard.emptySinceNanos() == 0) {
                tick.shards.put(shard.shardId(), shard.withEmptySince(tick.now));
            } else if (tick.now - shard.emptySinceNanos() >= tick.config.emptyShardNanos()) {
                tick.shards.put(shard.shardId(), shard.withState(ShardRecord.State.DRAINING));
            }
        }
    }

    void deleteDrained(final Tick tick) {
        for (ShardRecord shard : List.copyOf(tick.shards.values())) {
            if (shard.state() == ShardRecord.State.DRAINING && shard.isEmpty()) {
                tick.shards.put(shard.shardId(), shard.withState(ShardRecord.State.DELETED));
            }
        }
    }
}
