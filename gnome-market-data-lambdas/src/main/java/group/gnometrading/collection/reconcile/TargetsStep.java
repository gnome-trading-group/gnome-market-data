package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Registry;
import group.gnometrading.collection.ports.Store;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Rules become targets ({@link Evaluation}); retired ones are dropped, which takes them off their shards. */
final class TargetsStep {

    private final Store store;
    private final Registry registry;

    TargetsStep(final Store store, final Registry registry) {
        this.store = store;
        this.registry = registry;
    }

    void run(final Tick tick) {
        final List<Rule> rules = store.activeRules();
        final Set<Integer> ids = new HashSet<>(tick.targets.keySet());
        rules.forEach(rule -> ids.addAll(rule.streamIds()));
        final Map<Integer, StreamInfo> streams = registry.streams(ids);
        final Evaluation evaluation =
                new Evaluation(tick.config.defaultRetireGraceMinutes(), tick.config.endedRetireGraceMinutes());
        for (Target changed :
                evaluation.evaluate(rules, tick.targets, streams, tick.now).changed()) {
            tick.targets.put(changed.streamId(), changed);
        }
        for (Target target : List.copyOf(tick.targets.values())) {
            if (target.state() == Target.State.RETIRED) {
                tick.targets.remove(target.streamId());
                tick.retired++;
            }
        }
    }
}
