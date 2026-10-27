package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Alerts;
import group.gnometrading.collection.ports.Store;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Raises the collection alerts that hold this tick and resolves the ones the last tick left firing that don't: a shard
 * with no healthy task, streams not live or stale (one alert per source and region, not per stream: a region going
 * down shouldn't open a thousand), and sources whose accounts have no room.
 */
final class AlertStep {

    private static final Set<String> STALE_REASONS = Set.of("STALE", "QUARANTINED");

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
        tick.budgetShort.forEach((source, count) -> firing.put(
                "COLLECTION_BUDGET:" + source,
                new Firing(
                        "COLLECTION_BUDGET",
                        count + " " + source + " streams can't be placed: no market-data account has room",
                        Map.of("source", source, "unplaced", Integer.toString(count)))));
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
