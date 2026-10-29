package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Lease;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.ports.Registry;
import group.gnometrading.collection.ports.Store;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Session leases become target sources next to the rules: each ACTIVE lease is a rule {@code lease:<sessionId>} over
 * its streams, at its priority and tier, with a short retire grace so a restarting session keeps its shard.
 *
 * <p>A lease lives as long as its session: each tick reads every session not yet ended from the registry, once, and
 * releases the leases of sessions not among them. While the session is RUNNING its {@code expires_at} keeps moving
 * out; otherwise {@code expires_at} releases it. Each lease records its session's status as last read, so while the
 * registry can't be read a lease whose session was last seen STARTING or RUNNING outlives its {@code expires_at}
 * rather than cutting a running session off; {@link AlertStep} raises an alert once that has lasted
 * {@link #UNREADABLE_ALERT_NANOS}.
 *
 * <p>A released lease's streams stay published for the retire grace, as its targets stay on their shards: a session
 * being stopped still reads them while it winds down.
 */
final class LeaseStep {

    private static final Logger LOGGER = LogManager.getLogger(LeaseStep.class);

    static final int RETIRE_GRACE_MINUTES = 5;
    // The lease API's LIFETIME_S; lease-constants.json (test resources) holds the value both sides test against.
    static final long LIFETIME_SECONDS = TimeUnit.HOURS.toSeconds(2);
    // Extended once it has less than this left, so a running session's lease is written about once an hour, not every
    // tick.
    static final long EXTEND_BELOW_SECONDS = TimeUnit.HOURS.toSeconds(1);
    // How stale a lease's recorded session status may get before a tick rewrites it unchanged; bounds both the writes
    // and how late the unreadable-registry alert can be.
    static final long SEEN_REFRESH_SECONDS = TimeUnit.MINUTES.toSeconds(5);
    static final long UNREADABLE_ALERT_NANOS = TimeUnit.MINUTES.toNanos(15);
    static final String RUNNING = "RUNNING";
    // A session last seen in one of these may be trading, so only the registry, not expiry, ends its lease.
    static final Set<String> MAY_BE_TRADING = Set.of("STARTING", RUNNING);

    private final Store store;
    private final Registry registry;

    LeaseStep(final Store store, final Registry registry) {
        this.store = store;
        this.registry = registry;
    }

    void run(final Tick tick) {
        // Leases before sessions: the launcher writes a session's row before its lease, so every lease read here has
        // its session in the registry read after.
        final List<Lease> stored = store.leases();
        final List<Lease> holding = stored.stream().filter(Lease::isActive).toList();
        final Map<String, String> open = openSessions(holding);
        final long nowSeconds = TimeUnit.NANOSECONDS.toSeconds(tick.now);
        final List<Lease> active = new ArrayList<>();
        for (Lease lease : holding) {
            if (open == null ? keptBlind(tick, lease, nowSeconds) : kept(tick, lease, open, nowSeconds)) {
                active.add(lease);
            }
        }
        if (open == null && !active.isEmpty()) {
            tick.leaseHoldersUnreadableSinceNanos = TimeUnit.SECONDS.toNanos(active.stream()
                    .mapToLong(lease -> Math.max(lease.sessionSeenAtSeconds(), lease.createdAtSeconds()))
                    .max()
                    .orElse(nowSeconds));
        }
        final List<Rule> rules = new ArrayList<>(tick.rules);
        for (Lease lease : active) {
            rules.add(
                    new Rule(lease.sourceId(), lease.streams(), lease.priority(), lease.tier(), RETIRE_GRACE_MINUTES));
        }
        tick.rules = rules;
        tick.leases = List.copyOf(active);
        tick.leasedStreams = published(stored, active, nowSeconds);
    }

    /** The streams of the ACTIVE leases and of those released, this tick or earlier, less than the grace ago. */
    private static Set<Integer> published(final List<Lease> stored, final List<Lease> active, final long nowSeconds) {
        final long graceSeconds = TimeUnit.MINUTES.toSeconds(RETIRE_GRACE_MINUTES);
        final Set<Integer> published = new HashSet<>();
        for (Lease lease : stored) {
            if (lease.isActive() || nowSeconds - lease.releasedAtSeconds() < graceSeconds) {
                published.addAll(lease.streams());
            }
        }
        active.forEach(lease -> published.addAll(lease.streams()));
        return Set.copyOf(published);
    }

    /** Whether the lease stays, by its session's status in {@code open}; records that status on it now and then. */
    private boolean kept(final Tick tick, final Lease lease, final Map<String, String> open, final long nowSeconds) {
        final String status = open.get(lease.sessionId());
        if (status == null) {
            release(tick, lease, "SESSION_ENDED", nowSeconds);
            return false;
        }
        final boolean running = RUNNING.equals(status);
        if (!running && nowSeconds >= lease.expiresAtSeconds()) {
            release(tick, lease, "EXPIRED", nowSeconds);
            return false;
        }
        final long expiresAt = running && lease.expiresAtSeconds() - nowSeconds < EXTEND_BELOW_SECONDS
                ? nowSeconds + LIFETIME_SECONDS
                : lease.expiresAtSeconds();
        if (!status.equals(lease.sessionStatus())
                || nowSeconds - lease.sessionSeenAtSeconds() >= SEEN_REFRESH_SECONDS
                || expiresAt != lease.expiresAtSeconds()) {
            store.recordLeaseSession(lease.sessionId(), status, nowSeconds, expiresAt);
        }
        return true;
    }

    /** Whether the lease stays while the registry can't be read: until it expires, unless it may be trading. */
    private boolean keptBlind(final Tick tick, final Lease lease, final long nowSeconds) {
        if (nowSeconds < lease.expiresAtSeconds()
                || (lease.sessionStatus() != null && MAY_BE_TRADING.contains(lease.sessionStatus()))) {
            return true;
        }
        release(tick, lease, "EXPIRED", nowSeconds);
        return false;
    }

    /**
     * Every session not yet ended, by id, in the leases' mode when they share one; null when the registry can't be
     * read.
     */
    private Map<String, String> openSessions(final List<Lease> leases) {
        if (leases.isEmpty()) {
            return Map.of();
        }
        final Set<String> modes = leases.stream().map(Lease::mode).collect(Collectors.toSet());
        try {
            return registry.openSessionStatuses(
                    modes.size() == 1 ? modes.iterator().next() : null);
        } catch (RuntimeException error) {
            LOGGER.warn(
                    "Couldn't read the registry's sessions; leases are released only by expiry this tick: {}",
                    error.toString());
            return null;
        }
    }

    private void release(final Tick tick, final Lease lease, final String reason, final long nowSeconds) {
        store.releaseLease(lease.sessionId(), reason, nowSeconds);
        tick.releasedLeases++;
        LOGGER.info(
                "Released lease of session {} on {} streams: {}",
                lease.sessionId(),
                lease.streams().size(),
                reason);
    }
}
