package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.time.Clock;
import java.time.LocalDateTime;
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
    private final DynamoDbTable<TransformationJob> transformJobsTable;
    private final String mergedBucketName;
    private final String finalBucketName;
    private final Clock clock;

    public JobProcessorLambdaHandler() {
        this(
            Dependencies.getInstance().getS3Client(),
            Dependencies.getInstance().getTransformJobsTable(),
            Dependencies.getInstance().getObjectMapper(),
            Dependencies.getInstance().getMergedBucketName(),
            Dependencies.getInstance().getFinalBucketName(),
            Dependencies.getInstance().getClock()
        );
    }

    JobProcessorLambdaHandler(
            S3Client s3Client,
            DynamoDbTable<TransformationJob> transformJobsTable,
            ObjectMapper objectMapper,
            String mergedBucketName,
            String finalBucketName,
            Clock clock
    ) {
        this.s3Client = s3Client;
        this.transformJobsTable = transformJobsTable;
        this.objectMapper = objectMapper;
        this.mergedBucketName = mergedBucketName;
        this.finalBucketName = finalBucketName;
        this.clock = clock;
    }
    
    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {
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
        // Scan for pending jobs using enhanced client
        List<TransformationJob> jobs = getPendingJobs(schemaType, context);

        context.getLogger().log(String.format("Found %d pending jobs", jobs.size()));

        int processed = 0;
        for (TransformationJob job : jobs) {
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

    private List<TransformationJob> getPendingJobs(String schemaType, Context context) {
        // Build filter expression for PENDING status
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":status", AttributeValue.builder().s(TransformationStatus.PENDING.name()).build());

        String filterExpression = "#status = :status";
        Map<String, String> expressionNames = new HashMap<>();
        expressionNames.put("#status", "status");

        // Add schema type filter if specified
        if (schemaType != null) {
            expressionValues.put(":schemaType", AttributeValue.builder().s(schemaType).build());
            filterExpression += " AND schemaType = :schemaType";
        }

        // Build scan request using enhanced client
        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder()
                .filterExpression(Expression.builder()
                        .expression(filterExpression)
                        .expressionValues(expressionValues)
                        .expressionNames(expressionNames)
                        .build())
                .limit(MAX_JOBS_PER_INVOCATION)
                .build();

        // Execute scan and collect results
        PageIterable<TransformationJob> pages = transformJobsTable.scan(scanRequest);
        List<TransformationJob> jobs = new ArrayList<>();

        for (TransformationJob job : pages.items()) {
            jobs.add(job);
            if (jobs.size() >= MAX_JOBS_PER_INVOCATION) {
                break;
            }
        }

        return jobs;
    }
    
    private void processJob(TransformationJob job, Context context) throws Exception {
        context.getLogger().log(String.format("Processing job: listingId=%s, schemaType=%s, timestamp=%s",
                job.getListingId(), job.getSchemaType(), job.getTimestamp()));

        // Update job status to PROCESSING
        updateJobStatus(job, TransformationStatus.PROCESSING, context);

        // TODO: Construct source key from job information
        // For now, this is a placeholder - you'll need to implement the actual key construction
        // based on your S3 bucket structure for merged files
        String sourceKey = constructMergedFileKey(job);

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
        String transformedData = transformData(sourceData, job.getSchemaType(), context);

        // Upload transformed data to final bucket
        String outputKey = constructFinalFileKey(job);
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(finalBucketName)
                .key(outputKey)
                .contentType("text/csv")
                .build();

        s3Client.putObject(putObjectRequest, RequestBody.fromString(transformedData));

        // Update job status to COMPLETE
        updateJobStatus(job, TransformationStatus.COMPLETE, context);

        context.getLogger().log(String.format("Successfully processed job: listingId=%s, schemaType=%s, output=%s",
                job.getListingId(), job.getSchemaType(), outputKey));
    }

    private String constructMergedFileKey(TransformationJob job) {
        // TODO: Implement actual key construction based on your S3 structure
        // This is a placeholder implementation
        // Example: "merged/{listingId}/{year}/{month}/{day}/{hour}/{minute}/{schemaType}.zst"
        LocalDateTime timestamp = job.getTimestamp();
        return String.format("merged/%d/%d/%d/%d/%d/%d/%s.zst",
                job.getListingId(),
                timestamp.getYear(),
                timestamp.getMonthValue(),
                timestamp.getDayOfMonth(),
                timestamp.getHour(),
                timestamp.getMinute(),
                job.getSchemaType());
    }

    private String constructFinalFileKey(TransformationJob job) {
        // Construct output key for final bucket
        LocalDateTime timestamp = job.getTimestamp();
        return String.format("final/%d/%04d/%02d/%02d/%s_%04d%02d%02d_%02d%02d_%s.csv",
                job.getListingId(),
                timestamp.getYear(),
                timestamp.getMonthValue(),
                timestamp.getDayOfMonth(),
                job.getListingId(),
                timestamp.getYear(),
                timestamp.getMonthValue(),
                timestamp.getDayOfMonth(),
                timestamp.getHour(),
                timestamp.getMinute(),
                job.getSchemaType());
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

    private void updateJobStatus(TransformationJob job, TransformationStatus status, Context context) {
        // Update job status and processedAt timestamp
        job.setStatus(status);
        job.setProcessedAt(LocalDateTime.now(clock));

        // Use enhanced client to update the item
        transformJobsTable.updateItem(job);

        context.getLogger().log(String.format("Updated job status to %s: listingId=%s, schemaType=%s",
                status, job.getListingId(), job.getSchemaType()));
    }

    private void handleJobFailure(TransformationJob job, String errorMessage, Context context) {
        // Update job with error information
        job.setStatus(TransformationStatus.FAILED);
        job.setErrorMessage(errorMessage);
        job.setProcessedAt(LocalDateTime.now(clock));

        // Use enhanced client to update the item
        transformJobsTable.updateItem(job);

        context.getLogger().log(String.format("Job marked as FAILED: listingId=%s, schemaType=%s, error=%s",
                job.getListingId(), job.getSchemaType(), errorMessage));
    }
}

