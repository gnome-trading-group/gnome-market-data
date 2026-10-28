package group.gnometrading.data;

/**
 * A stretch of a stream with no collected data, from the market-data API's {@code collection/missing}: why, as a gap
 * reason ({@code TASK_LOST}, {@code STALE}, ...) or {@code NOT_COLLECTED} when nothing wanted the stream.
 */
public record MissingStretch(long fromNs, long toNs, String reason) {}
