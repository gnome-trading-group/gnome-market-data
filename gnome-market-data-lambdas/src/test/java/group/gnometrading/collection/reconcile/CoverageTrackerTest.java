package group.gnometrading.collection.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.StreamState;
import group.gnometrading.collection.model.TaskStatus;
import group.gnometrading.collection.model.Transition;
import group.gnometrading.collection.reconcile.CoverageTracker.Change;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class CoverageTrackerTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final long DAY = TimeUnit.DAYS.toNanos(1);
    // A day boundary plus some, so intervals that don't cross midnight stay away from one.
    private static final long T0 = 20_000 * DAY + TimeUnit.HOURS.toNanos(12);

    private static TaskStatus status(
            final long writtenAt, final List<StreamState> streams, final List<Transition> transitions) {
        return new TaskStatus("s1", "task-1", writtenAt, streams, transitions);
    }

    private static Interval open(final int streamId, final long from, final long to) {
        return new Interval(streamId, from, to, "s1", "task-1", true, null);
    }

    private static CoverageTracker.Result observe(final List<Interval> open, final TaskStatus status, final long now) {
        return CoverageTracker.observe("s1", "task-1", open, status, Set.of(), now);
    }

    @Test
    void aStreamThatBecameLiveOpensAnIntervalAtTheMomentItDid() {
        TaskStatus status = status(
                T0 + 15 * SECOND,
                List.of(new StreamState(7, "LIVE")),
                List.of(
                        new Transition(7, "NONE", "SUBSCRIBING", T0),
                        new Transition(7, "SUBSCRIBING", "LIVE", T0 + 3 * SECOND)));

        CoverageTracker.Result result = observe(List.of(), status, T0 + 20 * SECOND);

        assertEquals(List.of(open(7, T0 + 3 * SECOND, T0 + 15 * SECOND)), result.writes());
        assertEquals(List.of(new Change(7, T0 + 3 * SECOND, true, null)), result.changes());
    }

    @Test
    void anOpenIntervalIsExtendedOnlyEveryFewMinutes() {
        long every = CoverageTracker.EXTEND_EVERY_NANOS;
        TaskStatus soon = status(T0 + 30 * SECOND, List.of(new StreamState(7, "SUSPECT")), List.of());
        TaskStatus later = status(T0 + every, List.of(new StreamState(7, "SUSPECT")), List.of());

        assertTrue(
                observe(List.of(open(7, T0, T0)), soon, T0 + 31 * SECOND)
                        .writes()
                        .isEmpty(),
                "too soon to rewrite");
        CoverageTracker.Result result = observe(List.of(open(7, T0, T0)), later, T0 + every + SECOND);

        assertEquals(List.of(open(7, T0, T0 + every)), result.writes(), "suspect is still covered");
        assertTrue(result.changes().isEmpty());
    }

    @Test
    void anOpenIntervalIsStillSplitAtMidnightWhateverItsLastWrite() {
        long midnight = (T0 / DAY + 1) * DAY;
        TaskStatus status = status(midnight + 5 * SECOND, List.of(new StreamState(7, "LIVE")), List.of());

        CoverageTracker.Result result =
                observe(List.of(open(7, midnight - 60 * SECOND, midnight - 2 * SECOND)), status, midnight + 6 * SECOND);

        assertEquals(midnight, result.writes().get(0).toNanos());
        assertEquals("MIDNIGHT", result.writes().get(0).closeReason());
        assertEquals(
                List.of(open(7, midnight, midnight + 5 * SECOND)),
                result.writes().subList(1, 2));
    }

    @Test
    void aStaleStreamClosesWhenItWentStaleNotWhenTheStatusWasWritten() {
        TaskStatus status = status(
                T0 + 30 * SECOND,
                List.of(new StreamState(7, "STALE")),
                List.of(new Transition(7, "LIVE", "STALE", T0 + 22 * SECOND)));

        CoverageTracker.Result result = observe(List.of(open(7, T0, T0 + 15 * SECOND)), status, T0 + 31 * SECOND);

        Interval closed = result.writes().get(0);
        assertFalse(closed.open());
        assertEquals(T0 + 22 * SECOND, closed.toNanos());
        assertEquals("STALE", closed.closeReason());
        assertEquals(List.of(new Change(7, T0 + 22 * SECOND, false, "STALE")), result.changes());
    }

    @Test
    void aQuietOrEmptyStreamIsStillCovered() {
        long at = T0 + CoverageTracker.EXTEND_EVERY_NANOS;
        TaskStatus status = status(at, List.of(new StreamState(7, "EMPTY")), List.of());

        CoverageTracker.Result result = observe(List.of(open(7, T0, T0)), status, at + SECOND);

        assertTrue(result.writes().get(0).open(), "the venue has no book for it: nothing missed");
        assertTrue(result.changes().isEmpty());
    }

    @Test
    void aStreamGoneFromTheTaskClosesAsRemoved() {
        TaskStatus status =
                status(T0 + 30 * SECOND, List.of(), List.of(new Transition(7, "LIVE", "NONE", T0 + 20 * SECOND)));

        CoverageTracker.Result result = observe(List.of(open(7, T0, T0 + 15 * SECOND)), status, T0 + 31 * SECOND);

        assertEquals(T0 + 20 * SECOND, result.writes().get(0).toNanos());
        assertEquals(CoverageTracker.REMOVED, result.writes().get(0).closeReason());
    }

    @Test
    void aLostTasksIntervalClosesAtItsLastStatusNotItsTrailingStoredEnd() {
        TaskStatus last = status(T0 + 4 * 60 * SECOND, List.of(new StreamState(7, "LIVE")), List.of());

        CoverageTracker.Result result = observe(
                List.of(open(7, T0 - 60 * SECOND, T0)),
                last,
                T0 + 4 * 60 * SECOND + CoverageTracker.TASK_LOST_NANOS + SECOND);

        assertEquals(T0 + 4 * 60 * SECOND, result.writes().get(0).toNanos(), "the stored end trailed by 4 minutes");
        assertEquals(CoverageTracker.TASK_LOST, result.writes().get(0).closeReason());
    }

    @Test
    void aSilentTaskClosesEverythingAtTheLastConfirmedTime() {
        TaskStatus stale = status(T0, List.of(new StreamState(7, "LIVE")), List.of());

        CoverageTracker.Result result =
                observe(List.of(open(7, T0 - 60 * SECOND, T0)), stale, T0 + CoverageTracker.TASK_LOST_NANOS + SECOND);

        assertEquals(T0, result.writes().get(0).toNanos());
        assertEquals(CoverageTracker.TASK_LOST, result.writes().get(0).closeReason());
        assertEquals(
                CoverageTracker.TASK_LOST,
                observe(List.of(open(7, T0, T0)), null, T0).writes().get(0).closeReason(),
                "no status at all reads as lost");
    }

    @Test
    void anOpenIntervalSplitsAtUtcMidnight() {
        long midnight = 20_001 * DAY;
        TaskStatus status = status(midnight + 10 * SECOND, List.of(new StreamState(7, "LIVE")), List.of());

        CoverageTracker.Result result = observe(
                List.of(open(7, midnight - 60 * SECOND, midnight - 5 * SECOND)), status, midnight + 11 * SECOND);

        assertEquals(2, result.writes().size());
        assertEquals(midnight, result.writes().get(0).toNanos());
        assertFalse(result.writes().get(0).open());
        assertEquals(open(7, midnight, midnight + 10 * SECOND), result.writes().get(1));
        assertTrue(result.changes().isEmpty(), "still covered throughout");
    }

    @Test
    void aCoveredStreamWhoseChangeTheTaskForgotOpensAtTheStatusTime() {
        TaskStatus status = status(T0 + 15 * SECOND, List.of(new StreamState(7, "LIVE")), List.of());

        assertEquals(
                T0 + 15 * SECOND,
                observe(List.of(), status, T0 + 16 * SECOND).writes().get(0).fromNanos());
    }

    @Test
    void aStreamGoneFromATaskTheShardStillAssignsItClosesAsTaskStopped() {
        TaskStatus stopping = status(T0 + 30 * SECOND, List.of(), List.of());

        CoverageTracker.Result result = CoverageTracker.observe(
                "s1", "task-1", List.of(open(7, T0, T0 + 15 * SECOND)), stopping, Set.of(7), T0 + 31 * SECOND);

        assertEquals(CoverageTracker.TASK_STOPPED, result.writes().get(0).closeReason());
    }
}
