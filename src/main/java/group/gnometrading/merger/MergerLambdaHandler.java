package group.gnometrading.merger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.MarketDataEntry;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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
        context.getLogger().log("Received SQS event with " + event.getRecords().size() + " messages");

        try {
            Map<MarketDataEntry, List<MarketDataEntry>> keys = event.getRecords().stream()
                    .map(message -> extractKeysFromMessage(message, context))
                    .flatMap(map -> map.entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            keys.entrySet().parallelStream().forEach(entry -> {
                mergeEntries(entry.getKey(), entry.getValue(), context);
            });
        } catch (Exception e) {
            context.getLogger().log("Error processing messages: " + e.getMessage());
            throw new RuntimeException("Failed to process messages", e);
        }

        return null;
    }

    private void mergeEntries(MarketDataEntry mergedEntry, List<MarketDataEntry> rawEntries, Context context) {
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

    private Map<MarketDataEntry, List<MarketDataEntry>> extractKeysFromMessage(SQSMessage message, Context context) {
        String body = message.getBody();
        context.getLogger().log("Processing message: " + body);

        JsonNode s3Event;
        try {
            s3Event = objectMapper.readTree(body);
        } catch (Exception e) {
            context.getLogger().log("Error parsing message: " + e.getMessage());
            return Map.of();
        }
        
        JsonNode records = s3Event.get("Records");
        Map<MarketDataEntry, List<MarketDataEntry>> keys = new HashMap<>();
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                MarketDataEntry entry = parseS3Record(record, context);
                if (entry == null) {
                    continue;
                }

                assert entry.getEntryType() == MarketDataEntry.EntryType.RAW : "Expected raw entry, got " + entry.getEntryType();

                MarketDataEntry mergedEntry = new MarketDataEntry(entry.getSecurityId(), entry.getExchangeId(), entry.getSchemaType(), entry.getTimestamp(), MarketDataEntry.EntryType.AGGREGATED);
                keys.computeIfAbsent(mergedEntry, k -> new ArrayList<>()).add(entry);
            }
        }
        return keys;
    }
    
    private MarketDataEntry parseS3Record(JsonNode record, Context context) {
        JsonNode s3Info = record.get("s3");
        if (s3Info == null) {
            context.getLogger().log("No S3 information in record");
            return null;
        }
        
        String bucketName = s3Info.get("bucket").get("name").asText();
        String objectKey = s3Info.get("object").get("key").asText();
        long objectSize = s3Info.get("object").get("size").asLong();
        
        context.getLogger().log(String.format(
            "Processing S3 object: bucket=%s, key=%s, size=%d bytes",
            bucketName, objectKey, objectSize
        ));

        return MarketDataEntry.fromKey(objectKey);
    }
}

