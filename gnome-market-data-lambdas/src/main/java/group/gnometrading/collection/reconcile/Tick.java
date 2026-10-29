package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Lease;
import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.Settings;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Store;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One tick's working state, shared by its steps: the targets and shards as read and as they will be written, what the
 * shards' statuses showed, and what the tick did.
 */
final class Tick {

    static final Set<String> COVERED = Set.of("LIVE", "SUSPECT", "EMPTY");

    final Settings config;
    final long now;
    // The active rules, each with the streams it selects now (RuleResolver).
    List<Rule> rules = List.of();
    // The ACTIVE session leases (LeaseStep), already among the rules as sources, and the streams shards publish: every
    // stream they want, and those of leases released less than the retire grace ago.
    List<Lease> leases = List.of();
    Set<Integer> leasedStreams = Set.of();
    // When the leases' sessions were last read, if the registry couldn't be read this tick; 0 if it could.
    long leaseHoldersUnreadableSinceNanos;
    // What the registry says of every stream a rule wants or a target holds (TargetsStep).
    Map<Integer, StreamInfo> streamInfo = Map.of();
    final Map<Integer, Target> storedTargets;
    final Map<Integer, Target> targets;
    final Map<String, ShardRecord> storedShards = new LinkedHashMap<>();
    final Map<String, ShardRecord> shards = new LinkedHashMap<>();
    // Per shard: each stream's state in its fresh statuses, and when the shard last wrote any status.
    final Map<String, Map<Integer, String>> streamStates = new HashMap<>();
    final Map<String, Long> lastStatusNanos = new HashMap<>();
    // Per source: streams that couldn't be placed for want of account budget.
    final Map<String, Integer> budgetShort = new HashMap<>();
    // Per partition: streams that couldn't be placed because it is at its max_shards.
    final Map<Partition, Integer> capacityShort = new HashMap<>();
    int placed;
    int unplaced;
    int handedOff;
    int retired;
    int merged;
    int gaps;
    int alerts;
    int serviceErrors;
    int releasedLeases;

    Tick(final Store store, final Settings config, final long now) {
        this.config = config;
        this.now = now;
        this.storedTargets = store.targets();
        this.targets = new HashMap<>(storedTargets);
        for (ShardRecord shard : store.shards()) {
            storedShards.put(shard.shardId(), shard);
            shards.put(shard.shardId(), shard);
        }
    }

    /** Whether a fresh status of {@code shardId} (null: none) shows {@code streamId} collected. */
    boolean covered(final String shardId, final int streamId) {
        final String state = streamStates.getOrDefault(shardId, Map.of()).get(streamId);
        return state != null && COVERED.contains(state);
    }

    boolean exists(final String shardId) {
        final ShardRecord shard = shards.get(shardId);
        return shard != null && shard.state() != ShardRecord.State.DELETED;
    }
}
