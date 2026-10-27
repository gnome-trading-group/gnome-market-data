package group.gnometrading.quality.utils;

import group.gnometrading.md.Stream;
import group.gnometrading.schemas.Family;
import group.gnometrading.schemas.RecordType;

/**
 * The record type a stream's shard collects into its minute files: a book stream's is MBP_10 (gnome-orchestrator's
 * {@code ShardOrchestrator}), the only type the final bucket gets for it now derived types are converted on read.
 */
public final class CollectedRecordType {

    private CollectedRecordType() {}

    public static RecordType of(Stream stream) {
        final Family family = Family.forIdentifier(stream.family());
        if (family != Family.BOOK) {
            throw new IllegalStateException("Stream %d is a %s stream; only book streams are collected yet"
                    .formatted(stream.streamId(), stream.family()));
        }
        return RecordType.MBP_10;
    }
}
