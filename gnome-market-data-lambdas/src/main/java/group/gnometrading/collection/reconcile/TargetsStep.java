package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Registry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Resolved rules become targets ({@link Evaluation}); retired ones are dropped, which takes them off their shards.
 *
 * <p>What the registry says of each stream is kept for {@link #STREAM_INFO_TTL_NANOS} between ticks: a stream rarely
 * changes, and reading every target every minute is most of the reconciler's load on the registry. Streams of a source
 * whose ended subjects retire at once (an ended grace of 0, e.g. Hyperliquid, which drops a connection holding an
 * expired market) are read every tick.
 */
final class TargetsStep {

    static final long STREAM_INFO_TTL_NANOS = TimeUnit.MINUTES.toNanos(5);

    private record Cached(StreamInfo info, long readAtNanos) {}

    private final Registry registry;
    // Lives as long as the Lambda's container: a cold start reads everything once.
    private final Map<Integer, Cached> cache = new HashMap<>();

    TargetsStep(final Registry registry) {
        this.registry = registry;
    }

    void run(final Tick tick) {
        final Set<Integer> ids = new HashSet<>(tick.targets.keySet());
        tick.rules.forEach(rule -> ids.addAll(rule.streamIds()));
        final Map<Integer, StreamInfo> streams = streams(tick, ids);
        final Evaluation evaluation =
                new Evaluation(tick.config.defaultRetireGraceMinutes(), tick.config.endedRetireGraceMinutes());
        for (Target changed :
                evaluation.evaluate(tick.rules, tick.targets, streams, tick.now).changed()) {
            tick.targets.put(changed.streamId(), changed);
        }
        for (Target target : List.copyOf(tick.targets.values())) {
            if (target.state() == Target.State.RETIRED) {
                tick.targets.remove(target.streamId());
                tick.retired++;
            }
        }
    }

    private Map<Integer, StreamInfo> streams(final Tick tick, final Set<Integer> ids) {
        cache.keySet().retainAll(ids);
        final Set<Integer> stale = new HashSet<>();
        for (int id : ids) {
            final Cached cached = cache.get(id);
            if (cached == null
                    || tick.now - cached.readAtNanos() >= STREAM_INFO_TTL_NANOS
                    || tick.config
                                    .endedRetireGraceMinutes()
                                    .getOrDefault(cached.info().source(), -1)
                            == 0) {
                stale.add(id);
            }
        }
        if (!stale.isEmpty()) {
            registry.streams(stale).forEach((id, info) -> cache.put(id, new Cached(info, tick.now)));
        }
        final Map<Integer, StreamInfo> streams = new HashMap<>();
        for (int id : ids) {
            final Cached cached = cache.get(id);
            if (cached != null) {
                streams.put(id, cached.info());
            }
        }
        return streams;
    }
}
