package group.gnometrading.collection.model;

/** @param recordsPerSecond its collected records per second over the last minute; null if the status didn't say */
public record StreamState(int streamId, String state, Double recordsPerSecond) {

    public StreamState(final int streamId, final String state) {
        this(streamId, state, null);
    }
}
