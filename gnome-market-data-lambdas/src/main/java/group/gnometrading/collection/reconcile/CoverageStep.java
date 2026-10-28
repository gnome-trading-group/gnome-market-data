package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.StreamState;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.model.TaskStatus;
import group.gnometrading.collection.ports.Statuses;
import group.gnometrading.collection.ports.Store;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Shard statuses become coverage: each task's intervals ({@link CoverageTracker}), then each target's covered or
 * uncovered state and the gaps between. A target is covered while either shard it is on (or leaving, mid hand-off)
 * collects it, by any task, so a replaced task's late close doesn't uncover a stream its successor already covers.
 */
final class CoverageStep {

    // Shards write a status every 15 s: three missed and a task no longer counts as collecting anything.
    static final long STATUS_FRESH_NANOS = TimeUnit.SECONDS.toNanos(45);
    private static final String NO_STATUS = "NO_STATUS";

    /** What a shard's tasks showed of one stream this tick: its open coverage's start and its last covered moment. */
    private record Seen(long coveredFromNanos, long lastCoveredNanos, String reason) {
        static final Seen NONE = new Seen(Long.MAX_VALUE, 0, null);

        Seen merge(final Seen other) {
            return new Seen(
                    Math.min(coveredFromNanos, other.coveredFromNanos),
                    Math.max(lastCoveredNanos, other.lastCoveredNanos),
                    other.reason != null ? other.reason : reason);
        }
    }

    private final Store store;
    private final Statuses statuses;

    CoverageStep(final Store store, final Statuses statuses) {
        this.store = store;
        this.statuses = statuses;
    }

    void run(final Tick tick) {
        final Map<String, Map<Integer, Seen>> seen = new HashMap<>();
        for (ShardRecord shard : tick.shards.values()) {
            seen.put(shard.shardId(), observe(tick, shard));
        }
        for (Target target : List.copyOf(tick.targets.values())) {
            if (target.state() != Target.State.RETIRED && target.shardId() != null) {
                track(tick, target, seen);
            }
        }
    }

    private Map<Integer, Seen> observe(final Tick tick, final ShardRecord shard) {
        final Map<String, List<Interval>> openByTask = new HashMap<>();
        for (Interval interval : store.openIntervals(shard.shardId())) {
            openByTask
                    .computeIfAbsent(interval.taskId(), task -> new ArrayList<>())
                    .add(interval);
        }
        final Map<String, TaskStatus> statusByTask = new HashMap<>();
        for (TaskStatus status : statuses.latest(shard.shardId())) {
            statusByTask.put(status.taskId(), status);
            if (tick.now - status.writtenAtNanos() <= STATUS_FRESH_NANOS) {
                final Map<Integer, String> states =
                        tick.streamStates.computeIfAbsent(shard.shardId(), id -> new HashMap<>());
                status.streams()
                        .forEach(stream -> states.merge(
                                stream.streamId(), stream.state(), (a, b) -> Tick.COVERED.contains(a) ? a : b));
            }
            tick.lastStatusNanos.merge(shard.shardId(), status.writtenAtNanos(), Math::max);
        }
        final Set<String> tasks = new HashSet<>(openByTask.keySet());
        tasks.addAll(statusByTask.keySet());
        final Set<Integer> assigned = new HashSet<>(shard.streams());
        assigned.addAll(shard.draining());
        final Map<Integer, Seen> byStream = new HashMap<>();
        // Each task's newest status, for the streams it shows covered: an open interval's stored end trails (it is
        // written only every few minutes), the status doesn't.
        for (TaskStatus status : statusByTask.values()) {
            for (StreamState stream : status.streams()) {
                if (Tick.COVERED.contains(stream.state())) {
                    byStream.merge(
                            stream.streamId(), new Seen(Long.MAX_VALUE, status.writtenAtNanos(), null), Seen::merge);
                }
            }
        }
        for (String task : tasks) {
            final CoverageTracker.Result result = CoverageTracker.observe(
                    shard.shardId(),
                    task,
                    openByTask.getOrDefault(task, List.of()),
                    statusByTask.get(task),
                    assigned,
                    tick.now);
            result.writes().forEach(store::putInterval);
            record(result, byStream);
            stillOpen(openByTask.getOrDefault(task, List.of()), result, byStream);
        }
        return byStream;
    }

    /** Open intervals this tick didn't write (an extension is written only every few minutes) and didn't close. */
    private static void stillOpen(
            final List<Interval> open, final CoverageTracker.Result result, final Map<Integer, Seen> byStream) {
        final Set<Integer> written = new HashSet<>();
        result.writes().forEach(interval -> written.add(interval.streamId()));
        for (Interval interval : open) {
            if (!written.contains(interval.streamId())) {
                byStream.merge(
                        interval.streamId(), new Seen(interval.fromNanos(), interval.toNanos(), null), Seen::merge);
            }
        }
    }

    private static void record(final CoverageTracker.Result result, final Map<Integer, Seen> byStream) {
        for (Interval interval : result.writes()) {
            final Seen one =
                    new Seen(interval.open() ? interval.fromNanos() : Long.MAX_VALUE, interval.toNanos(), null);
            byStream.merge(interval.streamId(), one, Seen::merge);
        }
        for (CoverageTracker.Change change : result.changes()) {
            if (!change.covered()) {
                byStream.merge(change.streamId(), new Seen(Long.MAX_VALUE, 0, change.reason()), Seen::merge);
            }
        }
    }

    private void track(final Tick tick, final Target target, final Map<String, Map<Integer, Seen>> seen) {
        final boolean covered = tick.covered(target.shardId(), target.streamId())
                || tick.covered(target.drainingFromShardId(), target.streamId());
        final Seen view = seen(seen, target.shardId(), target.streamId())
                .merge(seen(seen, target.drainingFromShardId(), target.streamId()));
        if (covered && target.uncoveredSinceNanos() > 0) {
            resumed(tick, target, view);
        } else if (!covered && target.uncoveredSinceNanos() == 0) {
            final long at = view.lastCoveredNanos() > 0 ? view.lastCoveredNanos() : tick.now;
            tick.targets.put(target.streamId(), target.uncovered(at, reason(tick, target, view)));
        }
    }

    /** The gap runs to where the covering task's interval began, not to now: it may have covered for a while. */
    private void resumed(final Tick tick, final Target target, final Seen view) {
        final long end = view.coveredFromNanos() == Long.MAX_VALUE ? tick.now : view.coveredFromNanos();
        if (end > target.uncoveredSinceNanos()) {
            store.putGap(target.streamId(), target.uncoveredSinceNanos(), end, target.uncoveredReason());
            tick.gaps++;
        }
        tick.targets.put(target.streamId(), target.uncovered(end, null));
    }

    /** Why a stream isn't covered: its state where a fresh status shows it, else why its coverage closed. */
    private static String reason(final Tick tick, final Target target, final Seen view) {
        final String state =
                tick.streamStates.getOrDefault(target.shardId(), Map.of()).get(target.streamId());
        if (state != null) {
            return state;
        }
        return view.reason() != null ? view.reason() : NO_STATUS;
    }

    private static Seen seen(final Map<String, Map<Integer, Seen>> seen, final String shardId, final int streamId) {
        return shardId == null
                ? Seen.NONE
                : seen.getOrDefault(shardId, Map.of()).getOrDefault(streamId, Seen.NONE);
    }
}
