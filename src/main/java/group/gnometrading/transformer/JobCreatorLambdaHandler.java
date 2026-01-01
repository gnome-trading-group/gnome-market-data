package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.util.*;

/**
 * Lambda handler that creates transformation jobs for merged market data files.
 * Triggered by S3 events via SQS when new merged files are created.
 * Creates job entries in DynamoDB to be processed by JobProcessorLambdaHandler.
 */
public class JobCreatorLambdaHandler implements RequestHandler<SQSEvent, Void> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final String mergedBucketName;
    private final String transformJobsTableName;
    
    public JobCreatorLambdaHandler() {
        this.s3Client = S3Client.create();
        this.dynamoDbClient = DynamoDbClient.create();
        this.mergedBucketName = System.getenv("MERGED_BUCKET_NAME");
        this.transformJobsTableName = System.getenv("TRANSFORM_JOBS_TABLE_NAME");
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
        
        // Create transformation jobs for different schema types
        // For now, we'll create jobs for common transformations
        String[] schemaTypes = {"OHLC_1MIN", "OHLC_5MIN", "OHLC_15MIN", "OHLC_1HOUR", "OHLC_1DAY"};
        
        for (String schemaType : schemaTypes) {
            createTransformJob(listingId, date, key, schemaType, context);
        }
    }
    
    private void createTransformJob(String listingId, String date, String sourceKey, String schemaType, Context context) {
        String jobId = String.format("%s_%s_%s_%d", listingId, date, schemaType, Instant.now().toEpochMilli());
        
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("jobId", AttributeValue.builder().s(jobId).build());
        item.put("listingId", AttributeValue.builder().s(listingId).build());
        item.put("date", AttributeValue.builder().s(date).build());
        item.put("schemaType", AttributeValue.builder().s(schemaType).build());
        item.put("sourceKey", AttributeValue.builder().s(sourceKey).build());
        item.put("status", AttributeValue.builder().s("PENDING").build());
        item.put("createdAt", AttributeValue.builder().n(String.valueOf(Instant.now().getEpochSecond())).build());
        item.put("retryCount", AttributeValue.builder().n("0").build());
        
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(transformJobsTableName)
                .item(item)
                .conditionExpression("attribute_not_exists(jobId)") // Prevent duplicates
                .build();
        
        try {
            dynamoDbClient.putItem(putItemRequest);
            context.getLogger().log(String.format("Created transform job: %s for listing %s, date %s, schema %s", 
                    jobId, listingId, date, schemaType));
        } catch (ConditionalCheckFailedException e) {
            context.getLogger().log("Job already exists: " + jobId);
        }
    }
}

