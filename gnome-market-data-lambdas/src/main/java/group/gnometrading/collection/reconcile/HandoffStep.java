package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Target;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Advances hand-offs. The new shard collects without writing until it has covered the stream for the hand-off time;
 * then the cut is set, the old shard writing the minutes before it and the new one from it on, and once the old shard's
 * last minute has closed it lets the stream go. If the old shard stops covering first, the cut moves to the current
 * minute, so the new shard writes from it; the old one keeps the stream, cut, until it settles.
 */
final class HandoffStep {

    static final long MINUTE_NANOS = TimeUnit.MINUTES.toNanos(1);
    // A cut is at least this far ahead, so both shards poll it (every 10 s) before the minute starts.
    private static final long CUT_LEAD_NANOS = TimeUnit.SECONDS.toNanos(60);
    // The old shard lets a stream go this long after the cut: its last minute closes 5 s past the cut and uploads
    // once it stops, so nothing it should write is still open.
    private static final long CUT_SETTLE_NANOS = TimeUnit.MINUTES.toNanos(2);

    void run(final Tick tick) {
        for (Target target : List.copyOf(tick.targets.values())) {
            if (target.drainingFromShardId() == null) {
                continue;
            }
            if (!tick.exists(target.drainingFromShardId())) {
                complete(tick, target);
                continue;
            }
            final boolean newCovered = tick.covered(target.shardId(), target.streamId());
            final boolean oldCovered = tick.covered(target.drainingFromShardId(), target.streamId());
            final int thisMinute = (int) (tick.now / MINUTE_NANOS);
            if (target.cutMinute() == 0) {
                beforeCut(tick, target, newCovered, oldCovered, thisMinute);
            } else {
                afterCut(tick, target, newCovered, oldCovered, thisMinute);
            }
        }
    }

    private static void beforeCut(
            final Tick tick,
            final Target target,
            final boolean newCovered,
            final boolean oldCovered,
            final int thisMinute) {
        if (!newCovered) {
            tick.targets.put(target.streamId(), target.handoffCoveredSince(0));
        } else if (!oldCovered) {
            tick.targets.put(target.streamId(), target.cutAt(thisMinute));
        } else if (target.handoffCoveredSinceNanos() == 0) {
            tick.targets.put(target.streamId(), target.handoffCoveredSince(tick.now));
        } else if (tick.now - target.handoffCoveredSinceNanos() >= tick.config.handoffNanos()) {
            final int cut = (int) ((tick.now + CUT_LEAD_NANOS + MINUTE_NANOS - 1) / MINUTE_NANOS);
            tick.targets.put(target.streamId(), target.cutAt(cut));
        }
    }

    private static void afterCut(
            final Tick tick,
            final Target target,
            final boolean newCovered,
            final boolean oldCovered,
            final int thisMinute) {
        final long cutNanos = target.cutMinute() * MINUTE_NANOS;
        if (tick.now >= cutNanos + CUT_SETTLE_NANOS) {
            complete(tick, target);
        } else if (newCovered && !oldCovered && thisMinute < target.cutMinute()) {
            tick.targets.put(target.streamId(), target.cutAt(thisMinute));
        } else if (!newCovered && oldCovered && tick.now < cutNanos) {
            // The new shard lost the stream before the cut: the old one keeps writing, and it starts over.
            tick.targets.put(target.streamId(), target.cutAt(0).handoffCoveredSince(0));
        }
    }

    private static void complete(final Tick tick, final Target target) {
        tick.targets.put(target.streamId(), target.handedOff());
        tick.handedOff++;
    }
}
