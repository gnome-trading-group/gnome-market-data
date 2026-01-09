package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Lambda handler that processes transformation jobs on a schedule.
 * Reads pending jobs from DynamoDB, transforms merged data files,
 * and writes the transformed data to the final S3 bucket.
 */
public class JobProcessorLambdaHandler implements RequestHandler<Map<String, Object>, Void> {
    private static final int MAX_JOBS_PER_INVOCATION = 10;

    private final ObjectMapper objectMapper;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final String mergedBucketName;
    private final String finalBucketName;
    private final String transformJobsTableName;

    /**
     * No-argument constructor for Lambda runtime.
     * Uses the Dependencies singleton to obtain shared instances.
     */
    public JobProcessorLambdaHandler() {
        this(
            Dependencies.getInstance().getS3Client(),
            Dependencies.getInstance().getDynamoDbClient(),
            Dependencies.getInstance().getObjectMapper(),
            Dependencies.getInstance().getMergedBucketName(),
            Dependencies.getInstance().getFinalBucketName(),
            Dependencies.getInstance().getTransformJobsTableName()
        );
    }

    /**
     * Constructor for unit testing.
     * Allows injection of mock dependencies.
     *
     * @param s3Client S3 client for accessing merged and final data files
     * @param dynamoDbClient DynamoDB client for managing job records
     * @param objectMapper Jackson ObjectMapper for JSON parsing
     * @param mergedBucketName Name of the S3 bucket containing merged data
     * @param finalBucketName Name of the S3 bucket for final transformed data
     * @param transformJobsTableName Name of the DynamoDB table for transform jobs
     */
    JobProcessorLambdaHandler(
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            ObjectMapper objectMapper,
            String mergedBucketName,
            String finalBucketName,
            String transformJobsTableName) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.objectMapper = objectMapper;
        this.mergedBucketName = mergedBucketName;
        this.finalBucketName = finalBucketName;
        this.transformJobsTableName = transformJobsTableName;
    }
    
    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {
        // Get schemaType from event (passed by EventBridge rule)
        String schemaType = (String) event.get("schemaType");
        
        if (schemaType == null || schemaType.isEmpty()) {
            context.getLogger().log("No schemaType provided in event, processing all pending jobs");
            processPendingJobs(null, context);
        } else {
            context.getLogger().log("Processing jobs for schemaType: " + schemaType);
            processPendingJobs(schemaType, context);
        }
        
        return null;
    }
    
    private void processPendingJobs(String schemaType, Context context) {
        // Query for pending jobs
        List<Map<String, AttributeValue>> jobs = getPendingJobs(schemaType, context);
        
        context.getLogger().log(String.format("Found %d pending jobs", jobs.size()));
        
        int processed = 0;
        for (Map<String, AttributeValue> job : jobs) {
            if (processed >= MAX_JOBS_PER_INVOCATION) {
                context.getLogger().log("Reached max jobs per invocation, stopping");
                break;
            }
            
            try {
                processJob(job, context);
                processed++;
            } catch (Exception e) {
                context.getLogger().log("Error processing job: " + e.getMessage());
                handleJobFailure(job, e.getMessage(), context);
            }
        }
        
        context.getLogger().log(String.format("Processed %d jobs", processed));
    }
    
    private List<Map<String, AttributeValue>> getPendingJobs(String schemaType, Context context) {
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":status", AttributeValue.builder().s("PENDING").build());
        
        String filterExpression = "#status = :status";
        
        if (schemaType != null) {
            expressionValues.put(":schemaType", AttributeValue.builder().s(schemaType).build());
            filterExpression += " AND schemaType = :schemaType";
        }
        
        Map<String, String> expressionNames = new HashMap<>();
        expressionNames.put("#status", "status");
        
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(transformJobsTableName)
                .filterExpression(filterExpression)
                .expressionAttributeValues(expressionValues)
                .expressionAttributeNames(expressionNames)
                .limit(MAX_JOBS_PER_INVOCATION)
                .build();

        ScanResponse response = dynamoDbClient.scan(scanRequest);
        return response.items();
    }
    
    private void processJob(Map<String, AttributeValue> job, Context context) throws Exception {
        String jobId = job.get("jobId").s();
        String listingId = job.get("listingId").s();
        String date = job.get("date").s();
        String schemaType = job.get("schemaType").s();
        String sourceKey = job.get("sourceKey").s();
        
        context.getLogger().log(String.format("Processing job %s: listing=%s, date=%s, schema=%s", 
                jobId, listingId, date, schemaType));
        
        // Update job status to PROCESSING
        updateJobStatus(jobId, "PROCESSING", null, context);
        
        // Download source file from merged bucket
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(mergedBucketName)
                .key(sourceKey)
                .build();
        
        List<String> sourceData;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(s3Client.getObject(getObjectRequest)))) {
            sourceData = reader.lines().collect(Collectors.toList());
        }
        
        // Transform the data based on schema type
        String transformedData = transformData(sourceData, schemaType, context);
        
        // Upload transformed data to final bucket
        String outputKey = String.format("final/%s/%s/%s_%s_%s.csv", listingId, date, listingId, date, schemaType);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(finalBucketName)
                .key(outputKey)
                .contentType("text/csv")
                .build();
        
        s3Client.putObject(putObjectRequest, RequestBody.fromString(transformedData));

        // Update job status to COMPLETED
        updateJobStatus(jobId, "COMPLETED", outputKey, context);

        context.getLogger().log(String.format("Successfully processed job %s, output: %s", jobId, outputKey));
    }

    private String transformData(List<String> sourceData, String schemaType, Context context) {
        // TODO: Implement actual transformation logic based on schemaType
        // For now, this is a placeholder that returns the source data

        context.getLogger().log("Transforming data to schema: " + schemaType);

        // Example transformation logic for OHLC data
        if (schemaType.startsWith("OHLC_")) {
            return transformToOHLC(sourceData, schemaType, context);
        }

        // Default: return source data as-is
        return String.join("\n", sourceData);
    }

    private String transformToOHLC(List<String> sourceData, String schemaType, Context context) {
        // Extract interval from schema type (e.g., "OHLC_1MIN" -> 60000ms)
        long intervalMs = getIntervalMs(schemaType);

        // TODO: Implement OHLC aggregation logic
        // This is a placeholder implementation

        StringBuilder result = new StringBuilder();
        result.append("timestamp,open,high,low,close,volume\n");

        // For now, just return header
        // In a real implementation, you would:
        // 1. Parse source data (timestamp, price, volume)
        // 2. Group by time intervals
        // 3. Calculate OHLC values for each interval
        // 4. Format as CSV

        context.getLogger().log(String.format("OHLC transformation with interval %dms", intervalMs));

        return result.toString();
    }

    private long getIntervalMs(String schemaType) {
        // Extract interval from schema type
        if (schemaType.contains("1MIN")) return 60_000L;
        if (schemaType.contains("5MIN")) return 300_000L;
        if (schemaType.contains("15MIN")) return 900_000L;
        if (schemaType.contains("1HOUR")) return 3_600_000L;
        if (schemaType.contains("1DAY")) return 86_400_000L;
        return 60_000L; // Default to 1 minute
    }

    private void updateJobStatus(String jobId, String status, String outputKey, Context context) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("jobId", AttributeValue.builder().s(jobId).build());

        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("status", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s(status).build())
                .action(AttributeAction.PUT)
                .build());
        updates.put("updatedAt", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().n(String.valueOf(Instant.now().getEpochSecond())).build())
                .action(AttributeAction.PUT)
                .build());

        if (outputKey != null) {
            updates.put("outputKey", AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s(outputKey).build())
                    .action(AttributeAction.PUT)
                    .build());
        }

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(transformJobsTableName)
                .key(key)
                .attributeUpdates(updates)
                .build();

        dynamoDbClient.updateItem(updateRequest);
    }

    private void handleJobFailure(Map<String, AttributeValue> job, String errorMessage, Context context) {
        String jobId = job.get("jobId").s();
        int retryCount = Integer.parseInt(job.getOrDefault("retryCount",
                AttributeValue.builder().n("0").build()).n());

        retryCount++;

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("jobId", AttributeValue.builder().s(jobId).build());

        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put("retryCount", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().n(String.valueOf(retryCount)).build())
                .action(AttributeAction.PUT)
                .build());
        updates.put("lastError", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s(errorMessage).build())
                .action(AttributeAction.PUT)
                .build());
        updates.put("updatedAt", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().n(String.valueOf(Instant.now().getEpochSecond())).build())
                .action(AttributeAction.PUT)
                .build());

        // If retry count exceeds threshold, mark as FAILED
        if (retryCount >= 3) {
            updates.put("status", AttributeValueUpdate.builder()
                    .value(AttributeValue.builder().s("FAILED").build())
                    .action(AttributeAction.PUT)
                    .build());
            context.getLogger().log(String.format("Job %s marked as FAILED after %d retries", jobId, retryCount));
        } else {
            context.getLogger().log(String.format("Job %s failed, retry count: %d", jobId, retryCount));
        }

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(transformJobsTableName)
                .key(key)
                .attributeUpdates(updates)
                .build();

        dynamoDbClient.updateItem(updateRequest);
    }
}

