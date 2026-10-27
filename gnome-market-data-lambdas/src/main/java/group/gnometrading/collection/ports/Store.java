package group.gnometrading.collection.ports;

import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.Target;
import java.util.List;
import java.util.Map;

/** The control plane's tables. */
public interface Store {
    List<Rule> activeRules();

    Map<Integer, Target> targets();

    void putTarget(Target target);

    void deleteTarget(int streamId);

    /** Every shard not yet deleted. */
    List<ShardRecord> shards();

    /**
     * Writes {@code shard} if the stored generation is still {@code expectedGeneration} (-1: not stored yet);
     * false if another write got there first, which the next tick reconciles.
     */
    boolean putShard(ShardRecord shard, long expectedGeneration);

    List<Interval> openIntervals(String shardId);

    void putInterval(Interval interval);

    void putGap(int streamId, long fromNanos, long toNanos, String reason);

    /** The alert keys the last tick left firing, so a tick resolves the ones whose condition cleared. */
    Map<String, String> firingAlerts();

    void putFiringAlerts(Map<String, String> typeByKey);
}
