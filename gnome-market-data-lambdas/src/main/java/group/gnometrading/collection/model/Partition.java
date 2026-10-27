package group.gnometrading.collection.model;

/**
 * Which shards a stream may share: its collection region, its source's kind, and the target's priority and tier
 * (decision 23). Placement never mixes partitions.
 */
public record Partition(String region, String sourceKind, Priority priority, String tier) {}
