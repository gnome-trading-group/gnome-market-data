package group.gnometrading.quality.utils;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.md.Stream;
import group.gnometrading.schemas.Family;
import group.gnometrading.schemas.RecordType;
import java.util.List;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The record type a stream's gateway publishes and its collectors write. Only collected records are merged, so it is
 * the one record type the merged bucket holds for the stream.
 */
public final class CollectedRecordType {

    private CollectedRecordType() {}

    public static RecordType of(S3Client s3Client, String mergedBucket, Stream stream) {
        List<RecordType> recordTypes = StreamEntry.getRecordTypes(
                s3Client, mergedBucket, stream.streamId(), Family.forIdentifier(stream.family()));
        if (recordTypes.size() != 1) {
            throw new IllegalStateException("Stream %d has merged files of %s; expected one record type"
                    .formatted(stream.streamId(), recordTypes.isEmpty() ? "no record type" : recordTypes));
        }
        return recordTypes.get(0);
    }
}
