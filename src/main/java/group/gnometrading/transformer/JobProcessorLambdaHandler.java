package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import group.gnometrading.Dependencies;
import group.gnometrading.MarketDataEntry;
import group.gnometrading.SecurityMaster;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.converters.SchemaBulkConverter;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import group.gnometrading.sm.Listing;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Lambda handler that processes transformation jobs on a schedule.
 * Reads pending jobs from DynamoDB, transforms merged data files,
 * and writes the transformed data to the final S3 bucket.
 */
public class JobProcessorLambdaHandler implements RequestHandler<Map<String, Object>, Void> {

    private static final int MAX_JOBS_PER_INVOCATION = 100;

    private final S3Client s3Client;
    private final DynamoDbTable<TransformationJob> transformJobsTable;
    private final SecurityMaster securityMaster;
    private final String mergedBucketName;
    private final String finalBucketName;
    private final Clock clock;

    public JobProcessorLambdaHandler() {
        this(
            Dependencies.getInstance().getS3Client(),
            Dependencies.getInstance().getTransformJobsTable(),
            Dependencies.getInstance().getSecurityMaster(),
            Dependencies.getInstance().getMergedBucketName(),
            Dependencies.getInstance().getFinalBucketName(),
            Dependencies.getInstance().getClock()
        );
    }

    JobProcessorLambdaHandler(
            S3Client s3Client,
            DynamoDbTable<TransformationJob> transformJobsTable,
            SecurityMaster securityMaster,
            String mergedBucketName,
            String finalBucketName,
            Clock clock
    ) {
        this.s3Client = s3Client;
        this.transformJobsTable = transformJobsTable;
        this.securityMaster = securityMaster;
        this.mergedBucketName = mergedBucketName;
        this.finalBucketName = finalBucketName;
        this.clock = clock;
    }
    
    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {
        String rawSchemaType = (String) event.get("schemaType");
        
        if (rawSchemaType == null || rawSchemaType.isEmpty()) {
            throw new IllegalArgumentException("schemaType is required");
        }

        SchemaType schemaType = SchemaType.findById(rawSchemaType);
        context.getLogger().log("Processing jobs for schemaType: " + rawSchemaType);
        processPendingJobs(schemaType, context);
        
        return null;
    }
    
    private void processPendingJobs(SchemaType schemaType, Context context) {
        List<TransformationJob> jobs = getPendingJobs(schemaType);

        context.getLogger().log(String.format("Found %d pending jobs", jobs.size()));

        for (TransformationJob job : jobs) {
            try {
                processJob(job, context);
            } catch (Exception e) {
                context.getLogger().log("Error processing job: " + e.getMessage());
                handleJobFailure(job, e.getMessage(), context);
            }
        }
    }

    private List<TransformationJob> getPendingJobs(SchemaType schemaType) {
        Map<String, AttributeValue> expressionValues = Map.of(
                ":status", AttributeValue.builder().s(TransformationStatus.PENDING.name()).build(),
                ":schemaType", AttributeValue.builder().s(schemaType.getIdentifier()).build()
        );

        String filterExpression = "#status = :status AND #schemaType = :schemaType";
        Map<String, String> expressionNames = Map.of(
                "#status", "status",
                "#schemaType", "schemaType"
        );

        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder()
                .filterExpression(Expression.builder()
                        .expression(filterExpression)
                        .expressionValues(expressionValues)
                        .expressionNames(expressionNames)
                        .build())
                .limit(MAX_JOBS_PER_INVOCATION)
                .build();

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

        Window window = calculateWindow(job);
        Listing listing = securityMaster.getListing(job.getListingId());

        List<Schema> allSchemas = new ArrayList<>();
        for (LocalDateTime timestamp = window.start(); !timestamp.isAfter(window.end()); timestamp = timestamp.plusMinutes(1)) {
            MarketDataEntry entry = new MarketDataEntry(listing, timestamp, MarketDataEntry.EntryType.AGGREGATED);
            allSchemas.addAll(downloadSafely(entry, context));
        }

        if (allSchemas.isEmpty()) {
            context.getLogger().log("No schemas available to convert for job: " + job.getJobId());
            handleJobSuccess(job, context);
            return;
        }

        SchemaType originalSchemaType = listing.exchange().schemaType();
        SchemaBulkConverter<Schema, Schema> converter = (SchemaBulkConverter<Schema, Schema>) SchemaConversionRegistry.getBulkConverter(originalSchemaType, job.getSchemaType());
        List<Schema> convertedSchemas = converter.convert(allSchemas);

        MarketDataEntry outputEntry = new MarketDataEntry(
                listing.security().securityId(),
                listing.exchange().exchangeId(),
                job.getSchemaType(),
                job.getTimestamp(),
                MarketDataEntry.EntryType.AGGREGATED
        );
        outputEntry.saveToS3(s3Client, finalBucketName, convertedSchemas);

        handleJobSuccess(job, context);

        context.getLogger().log(String.format("Successfully processed job: listingId=%s, schemaType=%s, output=%s",
                job.getListingId(), job.getSchemaType(), outputEntry.getKey()));
    }

    private List<Schema> downloadSafely(MarketDataEntry entry, Context context) {
        try {
            return entry.loadFromS3(s3Client, mergedBucketName);
        } catch (NoSuchKeyException e) {
            context.getLogger().log("No S3 key found for " + entry);
            return List.of();
        }
    }

    private record Window(LocalDateTime start, LocalDateTime end) {}

    private Window calculateWindow(TransformationJob job) {
        return switch (job.getSchemaType()) {
            case MBO, MBP_10, MBP_1, BBO_1S, BBO_1M, TRADES, OHLCV_1S, OHLCV_1M ->
                new Window(job.getTimestamp(), job.getTimestamp());
            case OHLCV_1H ->
                new Window(job.getTimestamp().withMinute(0), job.getTimestamp().withMinute(59));
        };
    }

    private void handleJobSuccess(TransformationJob job, Context context) {
        job.setStatus(TransformationStatus.COMPLETE);
        job.setProcessedAt(LocalDateTime.now(clock));
        job.setExpiresAt(LocalDateTime.now(clock).plusWeeks(1).toEpochSecond(ZoneOffset.UTC));

        transformJobsTable.updateItem(job);

        context.getLogger().log(String.format("Job marked as COMPLETE: listingId=%s, schemaType=%s",
                job.getListingId(), job.getSchemaType()));
    }

    private void handleJobFailure(TransformationJob job, String errorMessage, Context context) {
        job.setStatus(TransformationStatus.FAILED);
        job.setErrorMessage(errorMessage);
        job.setProcessedAt(LocalDateTime.now(clock));
        job.setExpiresAt(LocalDateTime.now(clock).plusWeeks(1).toEpochSecond(ZoneOffset.UTC));

        transformJobsTable.updateItem(job);

        context.getLogger().log(String.format("Job marked as FAILED: listingId=%s, schemaType=%s, error=%s",
                job.getListingId(), job.getSchemaType(), errorMessage));
    }
}

