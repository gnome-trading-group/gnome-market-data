package group.gnometrading.collection.model;

/** One stream's time on one task; {@code toNanos} is the last moment known covered. */
public record Interval(
        int streamId, long fromNanos, long toNanos, String shardId, String taskId, boolean open, String closeReason) {

    public Interval extendTo(final long nanos) {
        return new Interval(streamId, fromNanos, Math.max(toNanos, nanos), shardId, taskId, true, null);
    }

    public Interval closeAt(final long nanos, final String reason) {
        return new Interval(streamId, fromNanos, Math.max(fromNanos, nanos), shardId, taskId, false, reason);
    }
}
