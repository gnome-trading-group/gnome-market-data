package group.gnometrading.collection.model;

/**
 * The most one shard of a tier carries: streams it collects, streams it publishes over Aeron, and collected records per
 * second, draining streams included. Measured by load tests on the tier's instance; each sits below the load at which
 * its latency targets failed.
 */
public record ShardCaps(int maxStreams, int maxPublished, double maxRecordsPerSecond) {

    /** These caps at {@code fill} of each, for a move that must leave its receivers with headroom. */
    public ShardCaps scaled(final double fill) {
        return new ShardCaps(
                (int) Math.floor(maxStreams * fill), (int) Math.floor(maxPublished * fill), maxRecordsPerSecond * fill);
    }
}
