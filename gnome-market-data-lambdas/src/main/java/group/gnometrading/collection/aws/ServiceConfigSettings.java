package group.gnometrading.collection.aws;

import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.Settings;
import group.gnometrading.common.config.MarketDataConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The reconciler's settings from the {@code market-data} Service Config. New shards run {@code roll_to_version} if set,
 * else the latest release; existing shards roll only when it is set.
 */
public final class ServiceConfigSettings implements Supplier<Settings> {

    private static final Logger LOGGER = LogManager.getLogger(ServiceConfigSettings.class);
    private static final String DEFAULT_TIER = "standard";

    private final Supplier<MarketDataConfig> config;
    private final Supplier<String> latestVersion;
    private final Set<String> regions;

    /**
     * @param latestVersion the latest orchestrator release (SSM {@code /gnome/orchestrator/latest-version})
     * @param regions the regions shards run in; shard counts for any other region are skipped
     */
    public ServiceConfigSettings(
            final Supplier<MarketDataConfig> config, final Supplier<String> latestVersion, final Set<String> regions) {
        this.config = config;
        this.latestVersion = latestVersion;
        this.regions = Set.copyOf(regions);
    }

    @Override
    public Settings get() {
        return of(config.get(), latestVersion, regions);
    }

    static Settings of(final MarketDataConfig config, final Supplier<String> latestVersion, final Set<String> regions) {
        final String rollTo = config.rollToVersion();
        final Map<Partition, Integer> maxShards = partitionShards("max_shards", config.maxShards(), regions);
        return new Settings(
                config.maxStreamsPerShard(),
                config.retireGraceMinutes(),
                config.endedRetireGraceMinutes(),
                rollTo != null ? rollTo : latestVersion.get(),
                rollTo,
                minutes(config.handoffMinutes()),
                minutes(config.emptyShardMinutes()),
                minutes(config.alerts().notLiveMinutes()),
                minutes(config.alerts().staleMinutes()),
                minutes(config.alerts().shardDownMinutes()),
                config.consolidation().enabled(),
                config.consolidation().dryRun(),
                minutes(config.consolidation().afterMinutes()),
                config.consolidation().maxFill(),
                minShards(partitionShards("min_shards", config.minShards(), regions), maxShards),
                maxShards);
    }

    /** {@code min_shards} with any count above the partition's {@code max_shards} lowered to it: the cap wins. */
    private static Map<Partition, Integer> minShards(
            final Map<Partition, Integer> minShards, final Map<Partition, Integer> maxShards) {
        final Map<Partition, Integer> capped = new HashMap<>(minShards);
        maxShards.forEach((partition, max) -> {
            final Integer min = capped.get(partition);
            if (min != null && min > max) {
                LOGGER.warn("min_shards {} for {} is above its max_shards {}; keeping {}", min, partition, max, max);
                capped.put(partition, max);
            }
        });
        capped.values().removeIf(count -> count <= 0);
        return Map.copyOf(capped);
    }

    /**
     * A {@code min_shards} or {@code max_shards} list by partition. A malformed entry, or one for a region shards don't
     * run in, is skipped rather than failing every tick on a typo in the config. A partition listed twice takes the
     * larger count for {@code min_shards} and the smaller for {@code max_shards}.
     */
    static Map<Partition, Integer> partitionShards(
            final String key, final List<MarketDataConfig.PartitionShards> entries, final Set<String> regions) {
        final Map<Partition, Integer> counts = new HashMap<>();
        if (entries == null) {
            return counts;
        }
        final boolean isMax = "max_shards".equals(key);
        for (MarketDataConfig.PartitionShards entry : entries) {
            try {
                if (!regions.contains(entry.region())) {
                    LOGGER.warn("Ignoring {} entry {}: shards don't run in region {}", key, entry, entry.region());
                    continue;
                }
                final Partition partition = new Partition(
                        entry.region(),
                        entry.sourceKind().toUpperCase(Locale.ROOT),
                        Priority.valueOf(entry.priority().toUpperCase(Locale.ROOT)),
                        entry.tier() == null ? DEFAULT_TIER : entry.tier());
                if (entry.count() < 0 || (!isMax && entry.count() == 0)) {
                    continue;
                }
                counts.merge(partition, entry.count(), isMax ? Math::min : Math::max);
            } catch (RuntimeException malformed) {
                LOGGER.warn("Ignoring malformed {} entry {}: {}", key, entry, malformed.toString());
            }
        }
        return Map.copyOf(counts);
    }

    private static long minutes(final int minutes) {
        return TimeUnit.MINUTES.toNanos(minutes);
    }
}
