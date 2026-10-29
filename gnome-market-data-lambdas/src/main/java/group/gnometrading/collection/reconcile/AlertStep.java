package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Lease;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Alerts;
import group.gnometrading.collection.ports.Store;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Raises the collection alerts that hold this tick and resolves the ones the last tick left firing that don't: a shard
 * with no healthy task, streams not live or stale (one alert per source and region, not per stream: a region going
 * down shouldn't open a thousand), sources whose accounts have no room, partitions at their {@code max_shards} with
 * streams waiting, leased streams no shard has taken, and leases whose sessions the registry hasn't answered for.
 */
final class AlertStep {

    private static final Set<String> STALE_REASONS = Set.of("STALE", "QUARANTINED");
    private static final String UNKNOWN = "unknown";
    // A cold start (host, task, jar, venue snapshot) takes 3-5 minutes. A session gives up waiting at 10, so this
    // leaves
    // someone half of that to act.
    static final long LEASE_UNPLACED_ALERT_NANOS = TimeUnit.MINUTES.toNanos(5);

    private final Store store;
    private final Alerts alerts;

    AlertStep(final Store store, final Alerts alerts) {
        this.store = store;
        this.alerts = alerts;
    }

    /** One alert: its type, title and labels. */
    private record Firing(String type, String title, Map<String, String> labels) {}

    void run(final Tick tick) {
        final Map<String, Firing> firing = new HashMap<>();
        shardsDown(tick, firing);
        streamsUncovered(tick, firing);
        leasedStreamsUnplaced(tick, firing);
        leaseHoldersUnreadable(tick, firing);
        tick.budgetShort.forEach((source, count) -> firing.put(
                "COLLECTION_BUDGET:" + source,
                new Firing(
                        "COLLECTION_BUDGET",
                        count + " " + source + " streams can't be placed: no market-data account has room",
                        Map.of("source", source, "unplaced", Integer.toString(count)))));
        tick.capacityShort.forEach((partition, count) -> firing.put(
                "COLLECTION_BUDGET:max_shards:" + partition.region() + ":" + partition.sourceKind() + ":"
                        + partition.priority() + ":" + partition.tier(),
                new Firing(
                        "COLLECTION_BUDGET",
                        count + " " + partition.priority() + " " + partition.sourceKind() + " streams in "
                                + partition.region() + " can't be placed: the partition is at its max_shards ("
                                + tick.config.maxShards(partition) + ")",
                        Map.of(
                                "region", partition.region(),
                                "source_kind", partition.sourceKind(),
                                "priority", partition.priority().name(),
                                "tier", partition.tier(),
                                "max_shards", Integer.toString(tick.config.maxShards(partition)),
                                "unplaced", Integer.toString(count)))));
        firing.forEach((key, alert) -> alerts.firing(alert.type(), key, alert.title(), alert.labels()));
        store.firingAlerts().forEach((key, type) -> {
            if (!firing.containsKey(key)) {
                alerts.resolved(type, key);
            }
        });
        final Map<String, String> typeByKey = new HashMap<>();
        firing.forEach((key, alert) -> typeByKey.put(key, alert.type()));
        store.putFiringAlerts(typeByKey);
        tick.alerts = firing.size();
    }

    private static void shardsDown(final Tick tick, final Map<String, Firing> firing) {
        for (ShardRecord shard : tick.shards.values()) {
            if (shard.state() == ShardRecord.State.DELETED
                    || tick.now - shard.createdAtNanos() < tick.config.shardDownNanos()) {
                continue;
            }
            final Long last = tick.lastStatusNanos.get(shard.shardId());
            if (last == null || tick.now - last > tick.config.shardDownNanos()) {
                firing.put(
                        "COLLECTOR_SHARD_DOWN:" + shard.shardId(),
                        new Firing(
                                "COLLECTOR_SHARD_DOWN",
                                "Shard " + shard.shardId() + " has no healthy task",
                                Map.of(
                                        "shard",
                                        shard.shardId(),
                                        "region",
                                        shard.partition().region())));
            }
        }
    }

    /**
     * Leased streams on no shard for {@link #LEASE_UNPLACED_ALERT_NANOS}, counted from when the stream went unplaced or
     * the earliest wanting lease was made, whichever is later, so a lease on a stream a rule already failed to place
     * gets the full wait. A leased stream with no target wanting it (its listing ended, or the registry doesn't know
     * it) can never be placed and counts from the lease. One alert per source and region.
     */
    private static void leasedStreamsUnplaced(final Tick tick, final Map<String, Firing> firing) {
        if (tick.leases.isEmpty()) {
            return;
        }
        final Map<Integer, Long> leasedSince = new HashMap<>();
        for (Lease lease : tick.leases) {
            final long since = TimeUnit.SECONDS.toNanos(lease.createdAtSeconds());
            lease.streams().forEach(stream -> leasedSince.merge(stream, since, Math::min));
        }
        final Map<String, Set<Integer>> unplaced = new HashMap<>();
        final Map<String, String[]> where = new HashMap<>();
        leasedSince.forEach((streamId, leased) -> {
            final Target target = tick.targets.get(streamId);
            if (unplacedTooLong(tick, target, leased)) {
                final String[] sourceAndRegion = sourceAndRegion(tick, streamId, target);
                final String key = "LEASED_STREAM_UNPLACED:" + sourceAndRegion[0] + ":" + sourceAndRegion[1];
                unplaced.computeIfAbsent(key, ignored -> new TreeSet<>()).add(streamId);
                where.putIfAbsent(key, sourceAndRegion);
            }
        });
        unplaced.forEach((key, streams) -> {
            final String source = where.get(key)[0];
            final String region = where.get(key)[1];
            firing.put(
                    key,
                    new Firing(
                            "LEASED_STREAM_UNPLACED",
                            streams.size() + " leased " + source + " streams in " + region + " have no shard",
                            Map.of(
                                    "source",
                                    source,
                                    "region",
                                    region,
                                    "streams",
                                    String.join(
                                            ",",
                                            streams.stream()
                                                    .map(String::valueOf)
                                                    .toList()))));
        });
    }

    /** Whether a stream leased since {@code leased} has been off every shard for the alert's wait. */
    private static boolean unplacedTooLong(final Tick tick, final Target target, final long leased) {
        final boolean wanted = target != null && target.state() == Target.State.DESIRED;
        if (wanted && target.shardId() != null) {
            return false;
        }
        final long unplacedSince = wanted && target.uncoveredSinceNanos() != 0 ? target.uncoveredSinceNanos() : leased;
        return tick.now - Math.max(leased, unplacedSince) >= LEASE_UNPLACED_ALERT_NANOS;
    }

    /** A stream's source and region, from its target, else the registry; "unknown" where neither says. */
    private static String[] sourceAndRegion(final Tick tick, final int streamId, final Target target) {
        if (target != null) {
            return new String[] {target.source(), target.partition().region()};
        }
        final StreamInfo info = tick.streamInfo.get(streamId);
        return info == null ? new String[] {UNKNOWN, UNKNOWN} : new String[] {info.source(), info.region()};
    }

    /** The registry unreadable for so long that leases of sessions that may be trading are outliving their expiry. */
    private static void leaseHoldersUnreadable(final Tick tick, final Map<String, Firing> firing) {
        final long since = tick.leaseHoldersUnreadableSinceNanos;
        if (since == 0 || tick.now - since < LeaseStep.UNREADABLE_ALERT_NANOS) {
            return;
        }
        firing.put(
                "LEASE_HOLDERS_UNREADABLE",
                new Firing(
                        "LEASE_HOLDERS_UNREADABLE",
                        "The registry hasn't said which of " + tick.leases.size()
                                + " leasing sessions are still running for "
                                + TimeUnit.NANOSECONDS.toMinutes(tick.now - since) + " minutes",
                        Map.of("leases", Integer.toString(tick.leases.size()))));
    }

    private static void streamsUncovered(final Tick tick, final Map<String, Firing> firing) {
        final Map<String, Integer> counts = new HashMap<>();
        final Map<String, Target> example = new HashMap<>();
        for (Target target : tick.targets.values()) {
            if (target.state() != Target.State.DESIRED || target.uncoveredSinceNanos() == 0) {
                continue;
            }
            final boolean stale = STALE_REASONS.contains(target.uncoveredReason());
            final long after = stale ? tick.config.staleAlertNanos() : tick.config.notLiveAlertNanos();
            if (tick.now - target.uncoveredSinceNanos() >= after) {
                final String key = (stale ? "STREAM_STALE" : "STREAM_NOT_LIVE") + ":" + target.source() + ":"
                        + target.partition().region();
                counts.merge(key, 1, Integer::sum);
                example.putIfAbsent(key, target);
            }
        }
        counts.forEach((key, count) -> {
            final Target target = example.get(key);
            final boolean stale = key.startsWith("STREAM_STALE");
            firing.put(
                    key,
                    new Firing(
                            stale ? "STREAM_STALE" : "STREAM_NOT_LIVE",
                            count + " " + target.source() + " streams in "
                                    + target.partition().region() + (stale ? " stale or quarantined" : " not live"),
                            Map.of(
                                    "source",
                                    target.source(),
                                    "region",
                                    target.partition().region())));
        });
    }
}
