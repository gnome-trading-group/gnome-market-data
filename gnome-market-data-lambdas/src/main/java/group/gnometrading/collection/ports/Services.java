package group.gnometrading.collection.ports;

import group.gnometrading.collection.model.ShardRecord;

/** The ECS service that runs each shard. */
public interface Services {
    /** Creates or updates the shard's service: one task on its version. Idempotent. */
    void ensure(ShardRecord shard);

    /** Deletes the shard's service. Idempotent. */
    void remove(ShardRecord shard);
}
