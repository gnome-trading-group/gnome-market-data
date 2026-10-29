package group.gnometrading.collection.ports;

import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.Lease;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.RuleStatus;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.Target;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** The control plane's tables. */
public interface Store {
    List<Rule> activeRules();

    /** Takes events out of an {@code event} rule, atomically; nothing if the rule was deleted meanwhile. */
    void removeEventIds(String ruleId, Set<Integer> eventIds);

    /** Records what a rule resolved to, on its item; nothing if the rule was deleted meanwhile. */
    void putRuleStatus(String ruleId, RuleStatus status);

    /** Every session lease, ACTIVE or RELEASED (until its TTL removes it); a malformed item is left out. */
    List<Lease> leases();

    /** Marks the lease RELEASED, with why; nothing if it was already released or deleted meanwhile. */
    void releaseLease(String sessionId, String reason, long nowSeconds);

    /**
     * Records an ACTIVE lease's session status as last read from the registry, and its {@code expires_at}; nothing if
     * it was released meanwhile.
     */
    void recordLeaseSession(String sessionId, String sessionStatus, long seenAtSeconds, long expiresAtSeconds);

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
