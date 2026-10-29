package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Registry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Places unplaced targets and moves those that must move ({@link Placement}, fullest fit), creating shards where none
 * fits. A target leaving a shard that is still there hands off from it; otherwise it is simply placed.
 */
final class PlacementStep {

    private static final Logger LOGGER = LogManager.getLogger(PlacementStep.class);

    private final Registry registry;
    private final Function<Partition, String> newShardId;

    PlacementStep(final Registry registry, final Function<Partition, String> newShardId) {
        this.registry = registry;
        this.newShardId = newShardId;
    }

    void run(final Tick tick) {
        final Map<String, List<Account>> accounts = registry.accounts();
        final List<ShardLoad> loads = loads(tick);
        final Map<Integer, String> moving = new HashMap<>();
        final List<Placement.Request> requests = requests(tick, accounts, moving);
        // A merging shard's streams go where the merge decided they fit, leaving its receivers the same headroom.
        final List<Placement.Request> merging = requests.stream()
                .filter(request -> isMerging(tick, moving.get(request.streamId())))
                .toList();
        requests.removeAll(merging);
        final Placement.Result result = new Placement(
                        registry.sources(), accounts, tick.config.maxStreamsPerShard(), newShardId)
                .place(loads, requests);
        final int maxFill = (int) Math.floor(tick.config.maxStreamsPerShard() * tick.config.consolidationMaxFill());
        final Placement.Result merged =
                new Placement(registry.sources(), accounts, maxFill, newShardId).place(loads, merging);
        apply(tick, result, loads, moving);
        apply(tick, merged, loads, moving);
        tick.unplaced = result.unplaced().size() + merged.unplaced().size();
    }

    /**
     * A shard draining with streams that isn't being rolled ({@link ConsolidationStep}): an emptied one has nothing to
     * move, and a rolled one is on an old version.
     */
    private static boolean isMerging(final Tick tick, final String shardId) {
        final ShardRecord shard = shardId == null ? null : tick.shards.get(shardId);
        final String rollTo = tick.config.rollToVersion();
        return shard != null
                && shard.state() == ShardRecord.State.DRAINING
                && (rollTo == null || rollTo.equals(shard.version()));
    }

    /** Unplaced targets, production first, then those that must move; {@code moving} gets each mover's shard. */
    private static List<Placement.Request> requests(
            final Tick tick, final Map<String, List<Account>> accounts, final Map<Integer, String> moving) {
        final List<Placement.Request> requests = new ArrayList<>();
        final List<Target> waiting = tick.targets.values().stream()
                .filter(target -> target.state() == Target.State.DESIRED)
                .sorted(Comparator.comparing(
                                (Target target) -> target.partition().priority())
                        .thenComparingInt(Target::streamId))
                .toList();
        for (Target target : waiting) {
            if (target.shardId() == null) {
                requests.add(new Placement.Request(target.streamId(), target.source(), target.partition()));
            } else if (target.drainingFromShardId() == null && mustMove(tick, target, accounts)) {
                requests.add(new Placement.Request(target.streamId(), target.source(), target.partition()));
                moving.put(target.streamId(), target.shardId());
            }
        }
        return requests;
    }

    private static void apply(
            final Tick tick,
            final Placement.Result result,
            final List<ShardLoad> loads,
            final Map<Integer, String> moving) {
        for (ShardLoad created : result.created()) {
            tick.shards.put(
                    created.shardId(),
                    ShardRecord.created(
                            created.shardId(), created.partition(), tick.config.newShardVersion(), tick.now));
        }
        for (ShardLoad load : loads) {
            final ShardRecord shard = tick.shards.get(load.shardId());
            if (shard != null && !shard.accounts().equals(load.accounts())) {
                tick.shards.put(shard.shardId(), shard.withAccounts(load.accounts()));
            }
        }
        for (Placement.Assigned assigned : result.assigned()) {
            assign(tick, assigned, moving.get(assigned.streamId()));
        }
        for (Placement.Unplaced unplaced : result.unplaced()) {
            if (unplaced.reason() == Placement.Unplaced.Reason.BUDGET) {
                tick.budgetShort.merge(tick.targets.get(unplaced.streamId()).source(), 1, Integer::sum);
            } else {
                LOGGER.warn("Stream {} can't be placed: its source isn't collectable", unplaced.streamId());
            }
        }
    }

    private static List<ShardLoad> loads(final Tick tick) {
        final List<ShardLoad> loads = new ArrayList<>();
        for (ShardRecord shard : tick.shards.values()) {
            loads.add(load(tick, shard, takesStreams(tick, shard)));
        }
        return loads;
    }

    /** What the shard carries per source, its draining streams included, as placement counts it. */
    static ShardLoad load(final Tick tick, final ShardRecord shard, final boolean accepting) {
        final ShardLoad load = new ShardLoad(shard.shardId(), shard.partition(), accepting, shard.accounts());
        final List<Integer> carried = new ArrayList<>(shard.streams());
        carried.addAll(shard.draining());
        for (int streamId : carried) {
            final Target target = tick.targets.get(streamId);
            if (target != null) {
                load.add(target.source());
            }
        }
        return load;
    }

    /**
     * An ACTIVE shard takes streams; during a roll only one already on the version rolled to, so a stream leaving a
     * rolling shard moves once, not onto another shard that will roll next.
     */
    static boolean takesStreams(final Tick tick, final ShardRecord shard) {
        final String rollTo = tick.config.rollToVersion();
        return shard.state() == ShardRecord.State.ACTIVE && (rollTo == null || rollTo.equals(shard.version()));
    }

    /**
     * Off a draining shard, one in another partition (a rule's priority or tier changed), or one bound to an account
     * no longer active for the stream's source.
     */
    private static boolean mustMove(final Tick tick, final Target target, final Map<String, List<Account>> accounts) {
        final ShardRecord shard = tick.shards.get(target.shardId());
        if (shard == null
                || shard.state() != ShardRecord.State.ACTIVE
                || !shard.partition().equals(target.partition())) {
            return true;
        }
        final String label = shard.accounts().get(target.source());
        if (label == null) {
            return false;
        }
        return accounts.getOrDefault(target.source(), List.of()).stream()
                .noneMatch(account -> account.label().equals(label) && account.active());
    }

    private static void assign(final Tick tick, final Placement.Assigned assigned, final String from) {
        final Target target = tick.targets.get(assigned.streamId());
        if (from != null && tick.exists(from)) {
            tick.targets.put(target.streamId(), target.handingOff(assigned.shardId(), from));
        } else {
            tick.targets.put(target.streamId(), target.onShard(assigned.shardId()));
        }
        tick.placed++;
    }
}
