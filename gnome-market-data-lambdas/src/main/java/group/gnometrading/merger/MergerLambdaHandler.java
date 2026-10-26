package group.gnometrading.merger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.SqsBatch;
import group.gnometrading.data.StreamEntry;
import group.gnometrading.schemas.StreamRecord;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Lambda handler for processing S3 object creation events from SQS queue.
 * This handler receives S3 event notifications via SQS and processes raw market data files.
 */
public final class MergerLambdaHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {

    private static final MergeStrategy MERGE_STRATEGY = new FullestCopyMergeStrategy();

    private static final Logger logger = LogManager.getLogger(MergerLambdaHandler.class);

    private final ObjectMapper objectMapper;
    private final S3Client s3Client;
    private final String inputBucket;
    private final String outputBucket;

    public MergerLambdaHandler() {
        this(
                Dependencies.getInstance().getObjectMapper(),
                Dependencies.getInstance().getS3Client(),
                Dependencies.getInstance().getRawBucketName(),
                Dependencies.getInstance().getMergedBucketName());
    }

    MergerLambdaHandler(ObjectMapper objectMapper, S3Client s3Client, String inputBucket, String outputBucket) {
        this.objectMapper = objectMapper;
        this.s3Client = s3Client;
        this.inputBucket = inputBucket;
        this.outputBucket = outputBucket;
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        return SqsBatch.forEachUnit(event, objectMapper, MergerLambdaHandler::mergedEntryOf, true, merged -> {
            mergeEntries(merged, rawKeysOf(merged));
        });
    }

    /** The merged minute a raw collector file belongs to; every raw copy of a minute merges into one. */
    private static StreamEntry mergedEntryOf(StreamEntry raw) {
        assert raw.getEntryType() == StreamEntry.EntryType.RAW : "Expected raw entry, got " + raw.getEntryType();
        return new StreamEntry(
                raw.getStreamId(), raw.getRecordType(), raw.getTimestamp(), StreamEntry.EntryType.AGGREGATED);
    }

    private void mergeEntries(StreamEntry mergedEntry, Set<StreamEntry> rawEntries) {
        logger.info("Merging {} entries into {}", rawEntries.size(), mergedEntry);

        Map<String, List<StreamRecord>> entries = new LinkedHashMap<>();
        for (StreamEntry entry : rawEntries) {
            entries.put(entry.getUuid(), entry.loadFromS3(s3Client, inputBucket));
        }

        int totalRecords = entries.values().stream().mapToInt(List::size).sum();
        var outputEntries = MERGE_STRATEGY.mergeRecords(entries);

        try {
            mergedEntry.saveToS3(s3Client, outputBucket, outputEntries);
        } catch (IOException e) {
            logger.error("Error trying to write merged key: {}", e.getMessage());
            throw new RuntimeException(e);
        }
        logger.info("Wrote {} (out of {}) records to merged key {}", outputEntries.size(), totalRecords, mergedEntry);
    }

    /** Every collector's raw copy of the minute, listed from the bucket rather than taken from the batch. */
    private Set<StreamEntry> rawKeysOf(StreamEntry mergedEntry) {
        return new HashSet<>(StreamEntry.getRawKeys(
                s3Client,
                inputBucket,
                mergedEntry.getStreamId(),
                mergedEntry.getTimestamp(),
                mergedEntry.getRecordType()));
    }
}
