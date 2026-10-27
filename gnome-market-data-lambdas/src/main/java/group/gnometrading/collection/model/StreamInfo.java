package group.gnometrading.collection.model;

/** What the registry says of a stream. {@code live} is false once its subject ended (listing inactive). */
public record StreamInfo(int streamId, String source, String sourceKind, String region, boolean live) {}
