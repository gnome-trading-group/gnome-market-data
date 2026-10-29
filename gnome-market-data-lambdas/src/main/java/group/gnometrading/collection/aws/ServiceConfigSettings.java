package group.gnometrading.collection.aws;

import group.gnometrading.collection.model.Settings;
import group.gnometrading.common.config.MarketDataConfig;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The reconciler's settings from the {@code market-data} Service Config. New shards run {@code roll_to_version} if set,
 * else the latest release; existing shards roll only when it is set.
 */
public final class ServiceConfigSettings implements Supplier<Settings> {

    private final Supplier<MarketDataConfig> config;
    private final Supplier<String> latestVersion;

    /** @param latestVersion the latest orchestrator release (SSM {@code /gnome/orchestrator/latest-version}) */
    public ServiceConfigSettings(final Supplier<MarketDataConfig> config, final Supplier<String> latestVersion) {
        this.config = config;
        this.latestVersion = latestVersion;
    }

    @Override
    public Settings get() {
        return of(config.get(), latestVersion);
    }

    static Settings of(final MarketDataConfig config, final Supplier<String> latestVersion) {
        final String rollTo = config.rollToVersion();
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
                config.consolidation().maxFill());
    }

    private static long minutes(final int minutes) {
        return TimeUnit.MINUTES.toNanos(minutes);
    }
}
