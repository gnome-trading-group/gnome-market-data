package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.SourceLimits;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Registry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Merges under-filled shards. A shard is mergeable while its streams would all fit on its partition's other shards,
 * each left at most {@code consolidationMaxFill} full and within every connection limit and account budget (a dry
 * run of {@link Placement}, with the shard itself taking nothing but still counting toward its accounts, as it does
 * while it hands off). One mergeable for {@code consolidateAfterNanos} drains like a rolled shard: placement hands its
 * streams to the others make-before-break, and once empty it is deleted. Retirements and rule churn otherwise leave a
 * partition spread over half-empty hosts.
 *
 * <p>A merge never takes a partition below its {@code min_shards}. One merge at a time, and none while a roll, a
 * hand-off or an unplaced stream is pending, so the dry run sees the room placement will find. The emptiest ready
 * shard goes first.
 */
final class ConsolidationStep {

    private static final Logger LOGGER = LogManager.getLogger(ConsolidationStep.class);
    private static final String SIMULATED_SHARD = "merge-simulation";

    private final Registry registry;

    ConsolidationStep(final Registry registry) {
        this.registry = registry;
    }

    void run(final Tick tick) {
        if (!tick.config.consolidationEnabled()) {
            forgetMergeable(tick);
            return;
        }
        final ShardRecord ready = markMergeable(tick);
        if (ready == null || busy(tick)) {
            return;
        }
        if (tick.config.consolidationDryRun()) {
            LOGGER.info(
                    "Would merge shard {} ({} streams) into its partition's other shards (dry run)",
                    ready.shardId(),
                    ready.streams().size());
            return;
        }
        LOGGER.info(
                "Merging shard {} ({} streams) into its partition's other shards",
                ready.shardId(),
                ready.streams().size());
        tick.shards.put(
                ready.shardId(), ready.withState(ShardRecord.State.DRAINING).withMergeableSince(0));
        tick.merged++;
    }

    private static void forgetMergeable(final Tick tick) {
        for (ShardRecord shard : List.copyOf(tick.shards.values())) {
            if (shard.mergeableSinceNanos() != 0) {
                tick.shards.put(shard.shardId(), shard.withMergeableSince(0));
            }
        }
    }

    /**
     * Records which ACTIVE shards are mergeable since when; returns the emptiest mergeable one once it has been for
     * long enough, else null. The emptiest is waited for rather than a fuller one that qualified first, so a merge
     * moves as few streams as it can.
     */
    private ShardRecord markMergeable(final Tick tick) {
        final Map<String, SourceLimits> sources = registry.sources();
        final Map<String, List<Account>> accounts = registry.accounts();
        final int maxFill = (int) Math.floor(tick.config.maxStreamsPerShard() * tick.config.consolidationMaxFill());
        final List<ShardRecord> mergeable = new ArrayList<>();
        for (ShardRecord shard : List.copyOf(tick.shards.values())) {
            if (shard.state() != ShardRecord.State.ACTIVE) {
                continue;
            }
            final boolean fits = !shard.isEmpty()
                    && !LifecycleStep.atMinimum(tick, shard)
                    && fitsElsewhere(tick, shard, sources, accounts, maxFill);
            final ShardRecord updated = since(shard, fits, tick.now);
            tick.shards.put(shard.shardId(), updated);
            if (fits) {
                mergeable.add(updated);
            }
        }
        return mergeable.stream()
                .min(Comparator.comparingInt(
                                (ShardRecord shard) -> shard.streams().size())
                        .thenComparing(ShardRecord::shardId))
                .filter(shard -> tick.now - shard.mergeableSinceNanos() >= tick.config.consolidateAfterNanos())
                .orElse(null);
    }

    /** The shard with its mergeable-since time started, kept, or cleared. */
    private static ShardRecord since(final ShardRecord shard, final boolean mergeable, final long now) {
        if (mergeable == (shard.mergeableSinceNanos() != 0)) {
            return shard;
        }
        return shard.withMergeableSince(mergeable ? now : 0);
    }

    private static boolean fitsElsewhere(
            final Tick tick,
            final ShardRecord shard,
            final Map<String, SourceLimits> sources,
            final Map<String, List<Account>> accounts,
            final int maxFill) {
        if (!shard.draining().isEmpty()) {
            return false;
        }
        final List<ShardLoad> loads = new ArrayList<>();
        for (ShardRecord other : tick.shards.values()) {
            if (other.partition().equals(shard.partition())) {
                final boolean accepting =
                        !other.shardId().equals(shard.shardId()) && PlacementStep.takesStreams(tick, other);
                loads.add(PlacementStep.load(tick, other, accepting));
            }
        }
        final List<Placement.Request> requests = new ArrayList<>();
        for (int streamId : shard.streams()) {
            final Target target = tick.targets.get(streamId);
            if (target == null) {
                return false;
            }
            requests.add(new Placement.Request(streamId, target.source(), shard.partition()));
        }
        final Placement.Result result = new Placement(
                        sources, accounts, maxFill, Map.of(), partition -> SIMULATED_SHARD)
                .place(loads, requests);
        return result.created().isEmpty() && result.unplaced().isEmpty();
    }

    /** A roll, a hand-off or a stream waiting for a shard: the dry run's room might not be there when placement runs. */
    private static boolean busy(final Tick tick) {
        return tick.shards.values().stream().anyMatch(shard -> moving(tick, shard))
                || tick.targets.values().stream()
                        .anyMatch(target -> target.state() == Target.State.DESIRED
                                && (target.shardId() == null || target.drainingFromShardId() != null));
    }

    /** Draining its streams away, or about to: a roll still has it on an old version. */
    private static boolean moving(final Tick tick, final ShardRecord shard) {
        if (shard.isEmpty()) {
            return false;
        }
        final String rollTo = tick.config.rollToVersion();
        return shard.state() == ShardRecord.State.DRAINING
                || (rollTo != null && shard.state() == ShardRecord.State.ACTIVE && !rollTo.equals(shard.version()));
    }
}
