package group.gnometrading.merger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.MarketDataEntry;
import group.gnometrading.S3Utils;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.*;

/**
 * Lambda handler for processing S3 object creation events from SQS queue.
 * This handler receives S3 event notifications via SQS and processes raw market data files.
 */
public class MergerLambdaHandler implements RequestHandler<SQSEvent, Void> {

    private final ObjectMapper objectMapper;
    private final S3Client s3Client;
    private final String inputBucket;
    private final String outputBucket;

    public MergerLambdaHandler() {
        this(
                Dependencies.getInstance().getObjectMapper(),
                Dependencies.getInstance().getS3Client(),
                Dependencies.getInstance().getRawBucketName(),
                Dependencies.getInstance().getMergedBucketName()
        );
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
            Set<MarketDataEntry> rawKeys = S3Utils.extractKeysFromS3Event(event, context, objectMapper);
            context.getLogger().log("Found " + rawKeys.size() + " raw keys in S3 event");

            Map<MarketDataEntry, Set<MarketDataEntry>> allKeys = aggregateRawKeys(rawKeys, context);

            allKeys.entrySet().parallelStream().forEach(entry -> {
                mergeEntries(entry.getKey(), entry.getValue(), context);
            });
        } catch (Exception e) {
            context.getLogger().log("Error processing messages: " + e.getMessage());
            throw new RuntimeException("Failed to process messages", e);
        }

        return null;
    }

    private void mergeEntries(MarketDataEntry mergedEntry, Set<MarketDataEntry> rawEntries, Context context) {
        context.getLogger().log("Merging " + rawEntries.size() + " entries into " + mergedEntry);

        Map<String, List<Schema>> entries = new LinkedHashMap<>();
        for (MarketDataEntry entry : rawEntries) {
            entries.put(entry.getUUID(), entry.loadFromS3(s3Client, inputBucket));
        }

        int totalRecords = entries.values().stream().mapToInt(List::size).sum();
        SchemaMergeStrategy strategy = getMergeStrategy(mergedEntry.getSchemaType());
        var outputEntries = strategy.mergeRecords(context.getLogger(), entries);

        try {
            mergedEntry.saveToS3(s3Client, outputBucket, outputEntries);
        } catch (IOException e) {
            context.getLogger().log("Error trying to write merged key: " + e.getMessage());
            throw new RuntimeException(e);
        }
        context.getLogger().log("Wrote " + outputEntries.size() + " (out of " + totalRecords + ") records to merged key " + mergedEntry);
    }

    private SchemaMergeStrategy getMergeStrategy(SchemaType schemaType) {
        return switch (schemaType) {
            case MBP_10 -> new MBP10MergeStrategy();
            default -> throw new IllegalArgumentException("Unsupported schema type: " + schemaType);
        };
    }

    private Map<MarketDataEntry, Set<MarketDataEntry>> aggregateRawKeys(Set<MarketDataEntry> rawKeys, Context context) {
        Set<MarketDataEntry> aggregatedKeys = new HashSet<>();
        for (MarketDataEntry entry : rawKeys) {
            assert entry.getEntryType() == MarketDataEntry.EntryType.RAW : "Expected raw entry, got " + entry.getEntryType();

            MarketDataEntry mergedEntry = new MarketDataEntry(entry.getSecurityId(), entry.getExchangeId(), entry.getSchemaType(), entry.getTimestamp(), MarketDataEntry.EntryType.AGGREGATED);
            aggregatedKeys.add(mergedEntry);
        }

        Map<MarketDataEntry, Set<MarketDataEntry>> keys = new HashMap<>();
        for (MarketDataEntry mergedEntry : aggregatedKeys) {
            List<MarketDataEntry> availableKeys = MarketDataEntry.getRawKeys(
                    s3Client,
                    inputBucket,
                    mergedEntry.getSecurityId(),
                    mergedEntry.getExchangeId(),
                    mergedEntry.getTimestamp(),
                    mergedEntry.getSchemaType()
            );
            keys.computeIfAbsent(mergedEntry, k -> new HashSet<>()).addAll(availableKeys);
        }

        return keys;
    }
}

