package group.gnometrading.collection.model;

import java.util.Set;

/**
 * A session's lease on streams ({@code collection-leases}, written by the lease API): while ACTIVE the streams are
 * collected at its priority and tier, and published over Aeron from whichever shard carries them. Leases are
 * non-exclusive; many sessions may lease one stream. A RELEASED lease stays readable until DynamoDB's TTL removes it.
 *
 * @param createdAtSeconds epoch seconds the lease was first made active
 * @param expiresAtSeconds epoch seconds after which it is released unless its session is RUNNING (the backstop when
 *     the registry can't say)
 * @param releasedAtSeconds epoch seconds it was released; 0 while ACTIVE
 * @param sessionStatus its session's status when the reconciler last read it from the registry; null if never read
 * @param sessionSeenAtSeconds epoch seconds of that read; 0 if never read
 */
public record Lease(
        String sessionId,
        Set<Integer> streams,
        Priority priority,
        String tier,
        String mode,
        Status status,
        long createdAtSeconds,
        long expiresAtSeconds,
        long releasedAtSeconds,
        String sessionStatus,
        long sessionSeenAtSeconds) {

    public enum Status {
        ACTIVE,
        RELEASED
    }

    /** The prefix of a lease's source id in a target's {@code wanted_by}, next to rule ids. */
    public static final String SOURCE_PREFIX = "lease:";

    /** An ACTIVE lease whose session the reconciler hasn't read yet, as the lease API writes it. */
    public static Lease active(
            final String sessionId,
            final Set<Integer> streams,
            final Priority priority,
            final String tier,
            final String mode,
            final long createdAtSeconds,
            final long expiresAtSeconds) {
        return new Lease(
                sessionId,
                streams,
                priority,
                tier,
                mode,
                Status.ACTIVE,
                createdAtSeconds,
                expiresAtSeconds,
                0,
                null,
                0);
    }

    public boolean isActive() {
        return status == Status.ACTIVE;
    }

    public Lease withSession(final String status, final long seenAtSeconds, final long expiresAt) {
        return new Lease(
                sessionId,
                streams,
                priority,
                tier,
                mode,
                this.status,
                createdAtSeconds,
                expiresAt,
                releasedAtSeconds,
                status,
                seenAtSeconds);
    }

    public Lease released(final long atSeconds) {
        return new Lease(
                sessionId,
                streams,
                priority,
                tier,
                mode,
                Status.RELEASED,
                createdAtSeconds,
                expiresAtSeconds,
                atSeconds,
                sessionStatus,
                sessionSeenAtSeconds);
    }

    public String sourceId() {
        return SOURCE_PREFIX + sessionId;
    }

    public static boolean isLeaseSource(final String sourceId) {
        return sourceId.startsWith(SOURCE_PREFIX);
    }
}
