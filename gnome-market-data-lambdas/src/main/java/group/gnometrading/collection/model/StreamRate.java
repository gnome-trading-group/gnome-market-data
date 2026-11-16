package group.gnometrading.collection.model;

/** A stream's collected records per second as a shard last reported it, and when the reconciler read it. */
public record StreamRate(double recordsPerSecond, long readAtNanos) {}
