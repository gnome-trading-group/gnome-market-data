package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Settings;
import group.gnometrading.collection.ports.Alerts;
import group.gnometrading.collection.ports.Registry;
import group.gnometrading.collection.ports.Services;
import group.gnometrading.collection.ports.Statuses;
import group.gnometrading.collection.ports.Store;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * One tick of the collection control plane, run every minute (reserved concurrency 1): rules resolve to streams and
 * become targets, shard statuses become coverage and gaps, targets are handed off and placed, shards rolled, merged,
 * scaled down and run, and alerts raised and resolved.
 *
 * <p>Targets are the source of truth: each tick rebuilds every shard's {@code streams}, {@code draining} and
 * {@code cuts} from them ({@link AssignmentStep}), so a shard item that missed a write is corrected by the next tick.
 */
public final class Reconciler {

    private static final Logger LOGGER = LogManager.getLogger(Reconciler.class);

    /** What a tick did, for its log line and tests. */
    public record Summary(
            int targets,
            int shards,
            int placed,
            int unplaced,
            int handedOff,
            int retired,
            int merged,
            int gaps,
            int alerts,
            int serviceErrors) {}

    private final Store store;
    private final Supplier<Settings> settings;
    private final RuleResolver ruleResolver;
    private final TargetsStep targetsStep;
    private final CoverageStep coverageStep;
    private final HandoffStep handoffStep = new HandoffStep();
    private final AssignmentStep assignmentStep = new AssignmentStep();
    private final LifecycleStep lifecycleStep = new LifecycleStep();
    private final ConsolidationStep consolidationStep;
    private final PlacementStep placementStep;
    private final PersistStep persistStep;
    private final AlertStep alertStep;

    public Reconciler(
            final Store store,
            final Registry registry,
            final Statuses statuses,
            final Services services,
            final Alerts alerts,
            final Supplier<Settings> settings,
            final Function<Partition, String> newShardId) {
        this.store = store;
        this.settings = settings;
        this.ruleResolver = new RuleResolver(store, registry);
        this.targetsStep = new TargetsStep(registry);
        this.coverageStep = new CoverageStep(store, statuses);
        this.consolidationStep = new ConsolidationStep(registry);
        this.placementStep = new PlacementStep(registry, newShardId);
        this.persistStep = new PersistStep(store, services);
        this.alertStep = new AlertStep(store, alerts);
    }

    public Summary tick(final long nowNanos) {
        final Tick tick = new Tick(store, settings.get(), nowNanos);
        ruleResolver.run(tick);
        targetsStep.run(tick);
        coverageStep.run(tick);
        handoffStep.run(tick);
        assignmentStep.run(tick);
        lifecycleStep.rollAndScaleDown(tick);
        consolidationStep.run(tick);
        placementStep.run(tick);
        assignmentStep.run(tick);
        lifecycleStep.deleteDrained(tick);
        persistStep.run(tick);
        alertStep.run(tick);
        final Summary summary = new Summary(
                tick.targets.size(),
                tick.shards.size(),
                tick.placed,
                tick.unplaced,
                tick.handedOff,
                tick.retired,
                tick.merged,
                tick.gaps,
                tick.alerts,
                tick.serviceErrors);
        LOGGER.info("Reconciled: {}", summary);
        return summary;
    }
}
