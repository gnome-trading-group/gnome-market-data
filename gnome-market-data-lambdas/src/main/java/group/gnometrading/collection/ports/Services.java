package group.gnometrading.collection.ports;

import group.gnometrading.collection.model.ShardRecord;

/** The ECS service that runs each shard. */
public interface Services {
    /** Creates or updates the shard's service: one task on its version. Idempotent. */
    void ensure(ShardRecord shard);

    /** Deletes the shard's service. Idempotent. */
    void remove(ShardRecord shard);

    /**
     * Whether the shard's service wants its task and no host can take it: none runs, and its task waits PROVISIONING for
     * a host or ECS says it is unable to place it. Asked only of production shards held to a zone that haven't reported
     * in time.
     */
    boolean unplaced(ShardRecord shard);
}
