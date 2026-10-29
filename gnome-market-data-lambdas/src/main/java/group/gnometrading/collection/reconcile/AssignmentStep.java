package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.Target;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Rebuilds every shard's {@code streams}, {@code draining}, {@code cuts} and {@code publish} from the targets and
 * leases, so a shard item that missed a write is corrected by the next tick. Mid hand-off the new shard is warming
 * (no cut yet) or writes from the cut; the old one writes everything until there is a cut, then up to it.
 */
final class AssignmentStep {

    void run(final Tick tick) {
        final Map<String, List<Integer>> streams = new HashMap<>();
        final Map<String, List<Integer>> draining = new HashMap<>();
        final Map<String, Map<Integer, Integer>> cuts = new HashMap<>();
        for (Target target : tick.targets.values()) {
            if (target.shardId() != null) {
                streams.computeIfAbsent(target.shardId(), id -> new ArrayList<>())
                        .add(target.streamId());
                addHandoff(target, draining, cuts);
            }
        }
        for (ShardRecord shard : List.copyOf(tick.shards.values())) {
            if (shard.state() == ShardRecord.State.DELETED) {
                continue;
            }
            final List<Integer> owned = sorted(streams.get(shard.shardId()));
            final List<Integer> leaving = sorted(draining.get(shard.shardId()));
            final Map<Integer, Integer> cut = Map.copyOf(cuts.getOrDefault(shard.shardId(), Map.of()));
            final List<Integer> publish = published(tick, owned, leaving);
            if (!owned.equals(shard.streams())
                    || !leaving.equals(shard.draining())
                    || !cut.equals(shard.cuts())
                    || !publish.equals(shard.publish())) {
                tick.shards.put(shard.shardId(), shard.withAssignment(owned, leaving, cut, publish));
            }
        }
    }

    private static void addHandoff(
            final Target target,
            final Map<String, List<Integer>> draining,
            final Map<String, Map<Integer, Integer>> cuts) {
        if (target.drainingFromShardId() == null) {
            return;
        }
        draining.computeIfAbsent(target.drainingFromShardId(), id -> new ArrayList<>())
                .add(target.streamId());
        cuts.computeIfAbsent(target.shardId(), id -> new HashMap<>())
                .put(target.streamId(), target.cutMinute() == 0 ? ShardRecord.WARMING : target.cutMinute());
        if (target.cutMinute() != 0) {
            cuts.computeIfAbsent(target.drainingFromShardId(), id -> new HashMap<>())
                    .put(target.streamId(), target.cutMinute());
        }
    }

    /**
     * The leased streams among those the shard carries. Draining ones too: mid hand-off a session reads the old shard
     * until the new one's first record.
     */
    private static List<Integer> published(final Tick tick, final List<Integer> owned, final List<Integer> leaving) {
        if (tick.leasedStreams.isEmpty()) {
            return List.of();
        }
        return Stream.concat(owned.stream(), leaving.stream())
                .filter(tick.leasedStreams::contains)
                .distinct()
                .sorted()
                .toList();
    }

    private static List<Integer> sorted(final List<Integer> ids) {
        return ids == null ? List.of() : ids.stream().sorted().toList();
    }
}
