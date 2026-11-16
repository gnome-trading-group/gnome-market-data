package group.gnometrading.collection.model;

/**
 * The most one shard of a tier carries, draining streams included: streams it collects, and its load. Published streams
 * (sent over Aeron) and collected records per second cost the same cores, so they count together: a shard's load is
 * {@code published / maxPublished + recordsPerSecond / maxRecordsPerSecond}, at most 1. Load tests on the tier's
 * instance measured the two adding up (a shard near both caps fails where one near either alone passes), and each cap
 * is the most of it alone, with a margin.
 */
public record ShardCaps(int maxStreams, int maxPublished, double maxRecordsPerSecond) {

    /** The share of the shard's room that {@code published} streams and {@code recordsPerSecond} take; 1 is full. */
    public double load(final int published, final double recordsPerSecond) {
        return published / (double) maxPublished + recordsPerSecond / maxRecordsPerSecond;
    }

    /** These caps at {@code fill} of each, for a move that must leave its receivers with headroom. */
    public ShardCaps scaled(final double fill) {
        return new ShardCaps(
                (int) Math.floor(maxStreams * fill), (int) Math.floor(maxPublished * fill), maxRecordsPerSecond * fill);
    }
}
