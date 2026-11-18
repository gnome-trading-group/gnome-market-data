package group.gnometrading.collection.model;

/**
 * Which shards a stream may share: its collection region, its source's kind, the target's priority and tier (decision
 * 23), and for PRODUCTION its exchange's home zone. Placement never mixes partitions, so production streams of venues
 * with different home zones never share a shard, while research shards (Spot, zone-free) mix a region's venues.
 *
 * @param zoneId the availability zone ID a production partition's shards run in (e.g. {@code use2-az2}); null for
 *     research partitions and for streams whose exchange has no home zone
 */
public record Partition(String region, String sourceKind, Priority priority, String tier, String zoneId) {

    /** A partition in no particular zone. */
    public Partition(final String region, final String sourceKind, final Priority priority, final String tier) {
        this(region, sourceKind, priority, tier, null);
    }
}
