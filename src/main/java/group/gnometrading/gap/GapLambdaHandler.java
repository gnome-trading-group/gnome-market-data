package group.gnometrading.gap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Lambda handler that detects gaps in merged market data files.
 * Triggered by S3 events via SQS when new merged files are created.
 * Analyzes the file to detect missing time intervals and records gaps in DynamoDB.
 */
public class GapLambdaHandler implements RequestHandler<SQSEvent, Void> {
    private final ObjectMapper objectMapper;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final String mergedBucketName;
    private final String gapsTableName;

    /**
     * No-argument constructor for Lambda runtime.
     * Uses the Dependencies singleton to obtain shared instances.
     */
    public GapLambdaHandler() {
        this(
            Dependencies.getInstance().getS3Client(),
            Dependencies.getInstance().getDynamoDbClient(),
            Dependencies.getInstance().getObjectMapper(),
            Dependencies.getInstance().getMergedBucketName(),
            Dependencies.getInstance().getGapsTableName()
        );
    }

    /**
     * Constructor for unit testing.
     * Allows injection of mock dependencies.
     *
     * @param s3Client S3 client for accessing merged data files
     * @param dynamoDbClient DynamoDB client for storing gap records
     * @param objectMapper Jackson ObjectMapper for JSON parsing
     * @param mergedBucketName Name of the S3 bucket containing merged data
     * @param gapsTableName Name of the DynamoDB table for gap records
     */
    GapLambdaHandler(
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            ObjectMapper objectMapper,
            String mergedBucketName,
            String gapsTableName) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.objectMapper = objectMapper;
        this.mergedBucketName = mergedBucketName;
        this.gapsTableName = gapsTableName;
    }
    
    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        for (SQSMessage message : event.getRecords()) {
            try {
                processMessage(message, context);
            } catch (Exception e) {
                context.getLogger().log("Error processing message: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        return null;
    }
    
    private void processMessage(SQSMessage message, Context context) throws Exception {
        String body = message.getBody();
        context.getLogger().log("Processing message: " + body);
        
        // Parse the S3 event notification from the SQS message body
        JsonNode s3Event = objectMapper.readTree(body);
        
        // S3 notifications can contain multiple records
        JsonNode records = s3Event.get("Records");
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                processS3Record(record, context);
            }
        }
    }
    
    private void processS3Record(JsonNode record, Context context) throws Exception {
        JsonNode s3 = record.get("s3");
        if (s3 == null) {
            context.getLogger().log("No S3 data in record, skipping");
            return;
        }
        
        String bucket = s3.get("bucket").get("name").asText();
        String key = s3.get("object").get("key").asText();
        long size = s3.get("object").get("size").asLong();
        
        context.getLogger().log(String.format("Processing S3 object: bucket=%s, key=%s, size=%d", bucket, key, size));
        
        // Parse key to extract metadata: merged/{listing_id}/{date}/{listing_id}_{date}.csv
        String[] parts = key.split("/");
        if (parts.length < 3 || !parts[0].equals("merged")) {
            context.getLogger().log("Invalid key format, skipping: " + key);
            return;
        }
        
        String listingId = parts[1];
        String date = parts[2];
        
        // Download and analyze the merged file for gaps
        List<Gap> gaps = detectGaps(bucket, key, listingId, date, context);
        
        // Store gaps in DynamoDB
        for (Gap gap : gaps) {
            storeGap(gap, context);
        }
        
        context.getLogger().log(String.format("Detected %d gaps for listing %s on %s", gaps.size(), listingId, date));
    }
    
    private List<Gap> detectGaps(String bucket, String key, String listingId, String date, Context context) throws Exception {
        List<Gap> gaps = new ArrayList<>();
        
        // Download the file from S3
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(s3Client.getObject(getObjectRequest)))) {
            
            // Read all timestamps from the file (assuming CSV with timestamp in first column)
            List<Long> timestamps = reader.lines()
                    .skip(1) // Skip header
                    .map(line -> line.split(",")[0])
                    .map(Long::parseLong)
                    .sorted()
                    .collect(Collectors.toList());
            
            if (timestamps.isEmpty()) {
                context.getLogger().log("No data in file: " + key);
                return gaps;
            }
            
            // Detect gaps (assuming 1-second intervals for market data)
            long expectedInterval = 1000; // 1 second in milliseconds
            
            for (int i = 1; i < timestamps.size(); i++) {
                long prev = timestamps.get(i - 1);
                long curr = timestamps.get(i);
                long diff = curr - prev;
                
                // If gap is larger than expected interval, record it
                if (diff > expectedInterval * 2) { // Allow some tolerance
                    gaps.add(new Gap(
                            listingId,
                            date,
                            prev,
                            curr,
                            diff,
                            Instant.now().getEpochSecond()
                    ));
                }
            }
        }
        
        return gaps;
    }
    
    private void storeGap(Gap gap, Context context) {
        String gapId = String.format("%s_%s_%d", gap.listingId, gap.date, gap.startTimestamp);

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("gapId", AttributeValue.builder().s(gapId).build());
        item.put("listingId", AttributeValue.builder().s(gap.listingId).build());
        item.put("date", AttributeValue.builder().s(gap.date).build());
        item.put("startTimestamp", AttributeValue.builder().n(String.valueOf(gap.startTimestamp)).build());
        item.put("endTimestamp", AttributeValue.builder().n(String.valueOf(gap.endTimestamp)).build());
        item.put("gapDuration", AttributeValue.builder().n(String.valueOf(gap.gapDuration)).build());
        item.put("detectedAt", AttributeValue.builder().n(String.valueOf(gap.detectedAt)).build());

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(gapsTableName)
                .item(item)
                .build();

        dynamoDbClient.putItem(putItemRequest);
        context.getLogger().log("Stored gap: " + gapId);
    }

    /**
     * Inner class representing a detected gap in market data.
     */
    private static class Gap {
        final String listingId;
        final String date;
        final long startTimestamp;
        final long endTimestamp;
        final long gapDuration;
        final long detectedAt;

        Gap(String listingId, String date, long startTimestamp, long endTimestamp, long gapDuration, long detectedAt) {
            this.listingId = listingId;
            this.date = date;
            this.startTimestamp = startTimestamp;
            this.endTimestamp = endTimestamp;
            this.gapDuration = gapDuration;
            this.detectedAt = detectedAt;
        }
    }
}

