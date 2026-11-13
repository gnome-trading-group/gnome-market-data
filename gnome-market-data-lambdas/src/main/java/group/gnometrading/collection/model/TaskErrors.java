package group.gnometrading.collection.model;

/**
 * What a task logged as unexpected errors (gnome-orchestrator's {@code ShardErrors}): every one since it started, those
 * in the last hour, and the latest line and when it came. A shard handles a venue closing a socket or a resync by
 * itself and logs it otherwise, so these are the ones someone should look at.
 */
public record TaskErrors(long total, int lastHour, String latest, long latestAtNanos) {

    /** A task that has logged none, or one too old to report them. */
    public static final TaskErrors NONE = new TaskErrors(0, 0, null, 0);

    /** Both tasks' together, as one shard's: the counts added, the latest line the later one's. */
    public TaskErrors and(final TaskErrors other) {
        final TaskErrors later = other.latestAtNanos > latestAtNanos ? other : this;
        return new TaskErrors(total + other.total, lastHour + other.lastHour, later.latest, later.latestAtNanos);
    }
}
