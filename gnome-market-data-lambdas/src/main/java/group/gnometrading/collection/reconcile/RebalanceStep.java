package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Settings;
import group.gnometrading.collection.model.ShardCaps;
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
 * Relieves shards over a cap. Every tick it measures each shard from its fresh statuses: the streams it carries, how
 * many it publishes, and their summed records per second, draining ones included. Only rates read count, so a shard
 * whose release doesn't report them is never over. An ACTIVE shard whose load ({@link ShardCaps#load}: published streams
 * and records per second together) is over 1 records since when; one over for {@code relieveAfterNanos}, outside its
 * cooldown, has streams handed off make-before-break onto shards with room (or a new one within {@code max_shards}),
 * until its load is at most {@code relieveTargetFill}, its receivers' at most that too.
 *
 * <p>Unleased streams go first, busiest first, so the fewest move and no session sees it; a leased stream moves only
 * when those aren't enough, the one taking most off the load first (it frees a published place and its rate). A stream
 * with no room stays where it is, and {@code SHARD_OVER_CAPACITY} stays up.
 *
 * <p>Runs after placement, so streams waiting for a shard take the room first, and only while no roll, merge or
 * hand-off is under way. One shard a tick, the furthest over first.
 */
final class RebalanceStep {

    private static final Logger LOGGER = LogManager.getLogger(RebalanceStep.class);
    private static final String SIMULATED_SHARD = "relief-simulation";

    /** What a shard carries by its fresh statuses, against its tier's caps. */
    record Usage(int streams, int published, double recordsPerSecond, ShardCaps caps) {
        boolean over() {
            return fill() > 1;
        }

        /** Its load against its tier's caps; over 1 is over. */
        double fill() {
            return caps.load(published, recordsPerSecond);
        }
    }

    private final Registry registry;
    private final Function<Partition, String> newShardId;

    RebalanceStep(final Registry registry, final Function<Partition, String> newShardId) {
        this.registry = registry;
        this.newShardId = newShardId;
    }

    void run(final Tick tick) {
        measure(tick);
        if (!tick.config.rebalance().enabled() || ConsolidationStep.moving(tick)) {
            return;
        }
        final ShardRecord shard = furthestOver(tick);
        if (shard != null) {
            relieve(tick, shard);
        }
    }

    /** Whether a merge or relief moved streams onto or off the shard within the shard cooldown. */
    static boolean coolingDown(final Tick tick, final ShardRecord shard) {
        return shard.rebalancedAtNanos() != 0
                && tick.now - shard.rebalancedAtNanos()
                        < tick.config.rebalance().shardCooldownNanos();
    }

    private static void measure(final Tick tick) {
        final Map<String, List<Integer>> carried = carried(tick);
        for (ShardRecord shard : List.copyOf(tick.shards.values())) {
            if (shard.state() != ShardRecord.State.DELETED) {
                measureShard(tick, shard, carried.getOrDefault(shard.shardId(), List.of()));
            }
        }
    }

    /** Per shard, the streams it collects: owned, and those it still has mid hand-off. */
    private static Map<String, List<Integer>> carried(final Tick tick) {
        final Map<String, List<Integer>> carried = new HashMap<>();
        for (Target target : tick.targets.values()) {
            if (target.shardId() != null) {
                carried.computeIfAbsent(target.shardId(), id -> new ArrayList<>())
                        .add(target.streamId());
            }
            if (target.drainingFromShardId() != null) {
                carried.computeIfAbsent(target.drainingFromShardId(), id -> new ArrayList<>())
                        .add(target.streamId());
            }
        }
        return carried;
    }

    private static void measureShard(final Tick tick, final ShardRecord shard, final List<Integer> streams) {
        int published = 0;
        double rate = 0;
        for (int streamId : streams) {
            if (tick.leasedStreams.contains(streamId)) {
                published++;
            }
            rate += tick.measuredRateOf(streamId);
        }
        final Usage usage = new Usage(streams.size(), published, rate, tick.config.caps(shard.partition()));
        tick.shardUsage.put(shard.shardId(), usage);
        final boolean over = shard.state() == ShardRecord.State.ACTIVE && usage.over();
        if (over && shard.overCapacitySinceNanos() == 0) {
            tick.shards.put(shard.shardId(), shard.withOverCapacitySince(tick.now));
        } else if (!over && shard.overCapacitySinceNanos() != 0) {
            tick.shards.put(shard.shardId(), shard.withOverCapacitySince(0));
        }
    }

    private static ShardRecord furthestOver(final Tick tick) {
        final long after = tick.config.rebalance().relieveAfterNanos();
        return tick.shards.values().stream()
                .filter(shard -> shard.state() == ShardRecord.State.ACTIVE
                        && PlacementStep.takesStreams(tick, shard)
                        && shard.overCapacitySinceNanos() != 0
                        && tick.now - shard.overCapacitySinceNanos() >= after
                        && !coolingDown(tick, shard))
                .max(Comparator.comparingDouble((ShardRecord shard) ->
                                tick.shardUsage.get(shard.shardId()).fill())
                        .thenComparing(ShardRecord::shardId, Comparator.reverseOrder()))
                .orElse(null);
    }

    private void relieve(final Tick tick, final ShardRecord shard) {
        final Settings.Rebalance settings = tick.config.rebalance();
        final Usage usage = tick.shardUsage.get(shard.shardId());
        final List<Target> chosen = choose(tick, shard, usage);
        if (chosen.isEmpty()) {
            LOGGER.info("Shard {} is over its caps ({}) but has no stream free to move", shard.shardId(), usage);
            return;
        }
        final List<Placement.Request> requests = new ArrayList<>();
        final Map<Integer, String> moving = new HashMap<>();
        for (Target target : chosen) {
            requests.add(PlacementStep.request(tick, target, shard.partition(), false, shard.shardId()));
            moving.put(target.streamId(), shard.shardId());
        }
        final List<ShardLoad> loads = PlacementStep.loads(tick);
        final Placement.Result result = new Placement(
                        registry.sources(),
                        registry.accounts(),
                        partition -> tick.config.caps(partition).scaled(settings.relieveTargetFill()),
                        tick.config.maxShards(),
                        settings.dryRun() ? partition -> SIMULATED_SHARD : newShardId)
                .place(loads, requests);
        if (settings.dryRun()) {
            LOGGER.info(
                    "Would relieve shard {} ({}): move {} of {} chosen streams, {} onto new shards (dry run)",
                    shard.shardId(),
                    usage,
                    result.assigned().size(),
                    chosen.size(),
                    result.assigned().stream()
                            .filter(assigned -> assigned.shardId().equals(SIMULATED_SHARD))
                            .count());
            return;
        }
        // A stream with no room stays where it is: it is collected, unlike one placement couldn't place at all.
        PlacementStep.apply(
                tick, new Placement.Result(result.assigned(), result.created(), List.of()), loads, moving, true);
        tick.relieved += result.assigned().size();
        LOGGER.info(
                "Relieving shard {} ({}): {} streams handing off, {} new shards, {} with no room",
                shard.shardId(),
                usage,
                result.assigned().size(),
                result.created().size(),
                result.unplaced().size());
    }

    /**
     * The streams to move: enough to bring the shard's load to {@code relieveTargetFill}, within the per-tick limits.
     * Streams mid hand-off or moved within the stream cooldown are left.
     */
    private static List<Target> choose(final Tick tick, final ShardRecord shard, final Usage usage) {
        final List<Target> leased = new ArrayList<>();
        final List<Target> unleased = new ArrayList<>();
        for (Target target : tick.targets.values()) {
            if (shard.shardId().equals(target.shardId())
                    && target.state() == Target.State.DESIRED
                    && target.drainingFromShardId() == null
                    && !recentlyMoved(tick, target)) {
                (tick.leasedStreams.contains(target.streamId()) ? leased : unleased).add(target);
            }
        }
        final Selection selection = new Selection(tick, usage);
        // Moving a stream takes its rate off the load, and a leased one its published place too: busiest first, so the
        // fewest move.
        final Comparator<Target> mostOffFirst = Comparator.comparingDouble(
                        (Target target) -> -selection.relief(target, tick.leasedStreams.contains(target.streamId())))
                .thenComparingInt(Target::streamId);
        unleased.sort(mostOffFirst);
        leased.sort(mostOffFirst);
        selection.shed(unleased);
        selection.shed(leased);
        return selection.chosen;
    }

    /** Streams chosen so far, and the shard's load as it will be once they have gone. */
    private static final class Selection {
        private final Tick tick;
        private final Settings.Rebalance settings;
        private final ShardCaps caps;
        private final List<Target> chosen = new ArrayList<>();
        private int published;
        private double rate;
        private int leasedMoves;

        Selection(final Tick tick, final Usage usage) {
            this.tick = tick;
            this.settings = tick.config.rebalance();
            this.caps = usage.caps();
            this.published = usage.published();
            this.rate = usage.recordsPerSecond();
        }

        /** What moving the stream takes off the shard's load. */
        double relief(final Target target, final boolean leased) {
            return caps.load(leased ? 1 : 0, tick.measuredRateOf(target.streamId()));
        }

        /** Streams in order until the shard's load is low enough; one that wouldn't lower it stays. */
        void shed(final List<Target> candidates) {
            for (Target target : candidates) {
                if (caps.load(published, rate) <= settings.relieveTargetFill()) {
                    return;
                }
                final boolean leased = tick.leasedStreams.contains(target.streamId());
                if (relief(target, leased) > 0 && canTake(leased) && !chosen.contains(target)) {
                    take(target, leased);
                }
            }
        }

        private boolean canTake(final boolean leased) {
            return chosen.size() < settings.maxMovesPerTick()
                    && (!leased || leasedMoves < settings.maxLeasedMovesPerTick());
        }

        private void take(final Target target, final boolean leased) {
            chosen.add(target);
            rate -= tick.measuredRateOf(target.streamId());
            if (leased) {
                leasedMoves++;
                published--;
            }
        }
    }

    private static boolean recentlyMoved(final Tick tick, final Target target) {
        return target.movedAtNanos() != 0
                && tick.now - target.movedAtNanos() < tick.config.rebalance().streamCooldownNanos();
    }
}
