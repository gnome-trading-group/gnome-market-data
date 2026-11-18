package group.gnometrading.collection.model;

/** What the registry says of a stream. {@code live} is false once its subject ended (listing inactive). */
public record StreamInfo(
        int streamId, String source, String sourceKind, String region, boolean live, String homeZoneId) {

    /** A stream whose exchange has no home zone. */
    public StreamInfo(
            final int streamId, final String source, final String sourceKind, final String region, final boolean live) {
        this(streamId, source, sourceKind, region, live, null);
    }
}
