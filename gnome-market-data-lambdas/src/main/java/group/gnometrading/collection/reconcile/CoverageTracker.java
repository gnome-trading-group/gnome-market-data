package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.StreamState;
import group.gnometrading.collection.model.TaskStatus;
import group.gnometrading.collection.model.Transition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Coverage intervals from one task's status (the reconciler's observe step): when each stream was being collected and
 * could be trusted, on that task. A stream is covered while the task reports it {@code LIVE}, {@code SUSPECT} (silent,
 * not yet shown wrong) or {@code EMPTY} (the venue has no book for it: nothing to miss).
 *
 * <ul>
 *   <li>An interval opens when the task's transitions show the stream became covered, else at the status's time.
 *   <li>Each status extends it to the status's time; it splits at UTC midnight, so a day's coverage is one query.
 *   <li>It closes when the stream stops being covered or leaves the task, at the transition's time if the task kept it,
 *       else at the last time it was confirmed; and at the last confirmed time, as {@link #TASK_LOST}, once the task's
 *       status is older than {@link #TASK_LOST_NANOS}.
 * </ul>
 *
 * <p>Pure: open intervals and a status in, interval writes and coverage changes out.
 */
public final class CoverageTracker {

    public static final long TASK_LOST_NANOS = TimeUnit.MINUTES.toNanos(5);
    public static final String TASK_LOST = "TASK_LOST";
    public static final String REMOVED = "REMOVED";
    private static final long DAY_NANOS = TimeUnit.DAYS.toNanos(1);
    private static final Set<String> COVERED = Set.of("LIVE", "SUSPECT", "EMPTY");

    /** A stream stopped or started being covered: the gaps between them are written from these. */
    public record Change(int streamId, long atNanos, boolean covered, String reason) {}

    /** Intervals to write (open ones updated, new ones, closed ones) and the coverage changes behind them. */
    public record Result(List<Interval> writes, List<Change> changes) {}

    private CoverageTracker() {}

    /** Applies {@code status} (null: none was found) to the task's {@code open} intervals. */
    public static Result observe(
            final String shardId,
            final String taskId,
            final List<Interval> open,
            final TaskStatus status,
            final long nowNanos) {
        final List<Interval> writes = new ArrayList<>();
        final List<Change> changes = new ArrayList<>();
        if (status == null || nowNanos - status.writtenAtNanos() > TASK_LOST_NANOS) {
            for (Interval interval : open) {
                close(interval, interval.toNanos(), TASK_LOST, writes, changes);
            }
            return new Result(writes, changes);
        }
        final Map<Integer, Interval> openByStream = new HashMap<>();
        for (Interval interval : open) {
            openByStream.put(interval.streamId(), interval);
        }
        for (StreamState stream : status.streams()) {
            observeStream(shardId, taskId, stream, openByStream.remove(stream.streamId()), status, writes, changes);
        }
        for (Interval removed : openByStream.values()) {
            close(removed, lastChange(status, removed.streamId(), false, removed.toNanos()), REMOVED, writes, changes);
        }
        return new Result(writes, changes);
    }

    private static void observeStream(
            final String shardId,
            final String taskId,
            final StreamState stream,
            final Interval current,
            final TaskStatus status,
            final List<Interval> writes,
            final List<Change> changes) {
        final long now = status.writtenAtNanos();
        final boolean covered = COVERED.contains(stream.state());
        if (covered && current != null) {
            extend(current, now, writes);
        } else if (covered) {
            final long from = lastChange(status, stream.streamId(), true, now);
            writes.add(new Interval(stream.streamId(), from, now, shardId, taskId, true, null));
            changes.add(new Change(stream.streamId(), from, true, null));
        } else if (current != null) {
            close(
                    current,
                    lastChange(status, stream.streamId(), false, current.toNanos()),
                    stream.state(),
                    writes,
                    changes);
        }
    }

    /** Extends to {@code now}, splitting at each UTC midnight passed since the interval opened. */
    private static void extend(final Interval interval, final long now, final List<Interval> writes) {
        Interval current = interval;
        long midnight = (current.fromNanos() / DAY_NANOS + 1) * DAY_NANOS;
        while (now >= midnight) {
            writes.add(current.closeAt(midnight, "MIDNIGHT"));
            current = new Interval(
                    current.streamId(), midnight, midnight, current.shardId(), current.taskId(), true, null);
            midnight += DAY_NANOS;
        }
        writes.add(current.extendTo(now));
    }

    private static void close(
            final Interval interval,
            final long at,
            final String reason,
            final List<Interval> writes,
            final List<Change> changes) {
        writes.add(interval.closeAt(at, reason));
        changes.add(new Change(interval.streamId(), at, false, reason));
    }

    /**
     * When the stream last became covered ({@code toCovered}) or stopped being covered, from the task's transitions;
     * {@code fallback} when the change is older than the task still remembers.
     */
    private static long lastChange(
            final TaskStatus status, final int streamId, final boolean toCovered, final long fallback) {
        long at = -1;
        for (Transition transition : status.transitions()) {
            if (transition.streamId() == streamId
                    && COVERED.contains(transition.to()) == toCovered
                    && COVERED.contains(transition.from()) != toCovered) {
                at = Math.max(at, transition.atNanos());
            }
        }
        return at < 0 ? fallback : at;
    }
}
