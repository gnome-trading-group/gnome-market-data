package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Services;
import group.gnometrading.collection.ports.Store;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Writes what changed and runs each shard's service. A shard item is written only if its stored generation hasn't
 * moved; a failure on one shard is logged and counted, and the others carry on.
 */
final class PersistStep {

    private static final Logger LOGGER = LogManager.getLogger(PersistStep.class);
    private static final String SHARD_DELETED = "SHARD_DELETED";

    private final Store store;
    private final Services services;

    PersistStep(final Store store, final Services services) {
        this.store = store;
        this.services = services;
    }

    void run(final Tick tick) {
        for (ShardRecord shard : tick.shards.values()) {
            try {
                persist(tick, shard);
            } catch (RuntimeException error) {
                tick.serviceErrors++;
                LOGGER.error("Shard {} failed this tick; the next one retries it", shard.shardId(), error);
            }
        }
        for (Target target : tick.targets.values()) {
            if (!target.equals(tick.storedTargets.get(target.streamId()))) {
                store.putTarget(target);
            }
        }
        for (int streamId : tick.storedTargets.keySet()) {
            if (!tick.targets.containsKey(streamId)) {
                store.deleteTarget(streamId);
            }
        }
    }

    /**
     * A deleted shard's service goes first, so a failed removal leaves the shard as stored, to be deleted again next
     * tick, rather than DELETED with its host still running.
     */
    private void persist(final Tick tick, final ShardRecord shard) {
        final boolean deleting = shard.state() == ShardRecord.State.DELETED;
        if (deleting && !shard.equals(tick.storedShards.get(shard.shardId()))) {
            services.remove(shard);
            closeCoverage(shard);
        }
        if (write(tick, shard) && !deleting) {
            services.ensure(shard);
        }
    }

    private boolean write(final Tick tick, final ShardRecord shard) {
        final ShardRecord stored = tick.storedShards.get(shard.shardId());
        if (shard.equals(stored)) {
            return true;
        }
        final long expected = stored == null ? -1 : stored.generation();
        if (store.putShard(shard.withGeneration(stored == null ? 1 : stored.generation() + 1), expected)) {
            return true;
        }
        LOGGER.warn("Shard {} changed under this tick; the next one reconciles it", shard.shardId());
        return false;
    }

    /** A deleted shard is never observed again, so its open intervals close at their last covered moment. */
    private void closeCoverage(final ShardRecord shard) {
        for (Interval interval : store.openIntervals(shard.shardId())) {
            store.putInterval(interval.closeAt(interval.toNanos(), SHARD_DELETED));
        }
    }
}
