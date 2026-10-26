package group.gnometrading.merger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.S3Utils;
import group.gnometrading.data.StreamEntry;
import group.gnometrading.schemas.StreamRecord;
import java.io.IOException;
import java.util.HashMap;
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
public final class MergerLambdaHandler implements RequestHandler<SQSEvent, Void> {

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
    public Void handleRequest(SQSEvent event, Context context) {
        try {
            Set<StreamEntry> rawKeys = S3Utils.extractKeysFromS3Event(event, objectMapper);
            logger.info("Found {} raw keys in S3 event", rawKeys.size());

            Map<StreamEntry, Set<StreamEntry>> allKeys = aggregateRawKeys(rawKeys);

            allKeys.entrySet().parallelStream().forEach(entry -> {
                mergeEntries(entry.getKey(), entry.getValue());
            });
        } catch (Exception e) {
            logger.error("Error processing messages: {}", e.getMessage());
            throw new RuntimeException("Failed to process messages", e);
        }

        return null;
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

    private Map<StreamEntry, Set<StreamEntry>> aggregateRawKeys(Set<StreamEntry> rawKeys) {
        Set<StreamEntry> aggregatedKeys = new HashSet<>();
        for (StreamEntry entry : rawKeys) {
            assert entry.getEntryType() == StreamEntry.EntryType.RAW
                    : "Expected raw entry, got " + entry.getEntryType();

            StreamEntry mergedEntry = new StreamEntry(
                    entry.getStreamId(), entry.getRecordType(), entry.getTimestamp(), StreamEntry.EntryType.AGGREGATED);
            aggregatedKeys.add(mergedEntry);
        }

        Map<StreamEntry, Set<StreamEntry>> keys = new HashMap<>();
        for (StreamEntry mergedEntry : aggregatedKeys) {
            List<StreamEntry> availableKeys = StreamEntry.getRawKeys(
                    s3Client,
                    inputBucket,
                    mergedEntry.getStreamId(),
                    mergedEntry.getTimestamp(),
                    mergedEntry.getRecordType());
            keys.computeIfAbsent(mergedEntry, k -> new HashSet<>()).addAll(availableKeys);
        }

        return keys;
    }
}
