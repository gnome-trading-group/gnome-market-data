package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import group.gnometrading.Dependencies;
import group.gnometrading.SecurityMaster;
import group.gnometrading.data.StreamEntry;
import group.gnometrading.md.Stream;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.schemas.converters.SchemaBulkConverter;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Lambda handler that processes transformation jobs on a schedule.
 * Reads pending jobs from DynamoDB, transforms merged data files,
 * and writes the transformed data to the final S3 bucket.
 */
public final class JobProcessorLambdaHandler implements RequestHandler<Map<String, Object>, Void> {

    private static final Logger logger = LogManager.getLogger(JobProcessorLambdaHandler.class);
    private static final int MAX_JOBS_PER_INVOCATION = 10_000;

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
                Dependencies.getInstance().getClock());
    }

    JobProcessorLambdaHandler(
            S3Client s3Client,
            DynamoDbTable<TransformationJob> transformJobsTable,
            SecurityMaster securityMaster,
            String mergedBucketName,
            String finalBucketName,
            Clock clock) {
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

        RecordType schemaType = RecordType.forIdentifier(rawSchemaType);
        logger.info("Processing jobs for schemaType: {}", rawSchemaType);
        processPendingJobs(schemaType);

        return null;
    }

    private void processPendingJobs(RecordType schemaType) {
        List<TransformationJob> jobs = getPendingJobs(schemaType);

        logger.info("Found {} pending jobs", jobs.size());

        for (TransformationJob job : jobs) {
            try {
                processJob(job);
            } catch (Exception e) {
                logger.error("Error processing job: {}", e.getMessage());
                handleJobFailure(job, e.getMessage());
            }
        }
    }

    private List<TransformationJob> getPendingJobs(RecordType schemaType) {
        DynamoDbIndex<TransformationJob> index = transformJobsTable.index("schemaType-status-index");

        QueryConditional queryConditional = QueryConditional.sortBeginsWith(Key.builder()
                .partitionValue(schemaType.identifier())
                .sortValue(TransformationStatus.PENDING.name())
                .build());

        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(queryConditional)
                .limit(MAX_JOBS_PER_INVOCATION)
                .build();

        SdkIterable<Page<TransformationJob>> pages = index.query(queryRequest);
        List<TransformationJob> jobs = new ArrayList<>();

        for (Page<TransformationJob> page : pages) {
            for (TransformationJob job : page.items()) {
                jobs.add(job);
                if (jobs.size() >= MAX_JOBS_PER_INVOCATION) {
                    return jobs;
                }
            }
        }

        return jobs;
    }

    private void processJob(TransformationJob job) throws Exception {
        logger.info(
                "Processing job: listingId={}, schemaType={}, timestamp={}",
                job.getListingId(),
                job.getRecordType(),
                job.getTimestamp());

        Window window = calculateWindow(job);
        Stream stream = securityMaster.streamForListing(job.getListingId());
        RecordType collected = job.getSourceRecordType();
        if (collected == null) {
            throw new IllegalStateException("Job " + job.getJobId() + " names no record type to convert from");
        }

        List<StreamRecord> allSchemas = new ArrayList<>();
        for (LocalDateTime timestamp = window.start();
                !timestamp.isAfter(window.end());
                timestamp = timestamp.plusMinutes(1)) {
            StreamEntry entry =
                    new StreamEntry(stream.streamId(), collected, timestamp, StreamEntry.EntryType.AGGREGATED);
            allSchemas.addAll(downloadSafely(entry));
        }

        if (allSchemas.isEmpty()) {
            logger.warn("No schemas available to convert for job: {}", job.getJobId());
            handleJobSuccess(job);
            return;
        }

        SchemaBulkConverter<StreamRecord, StreamRecord> converter = (SchemaBulkConverter<StreamRecord, StreamRecord>)
                SchemaConversionRegistry.getBulkConverter(collected, job.getRecordType());
        List<StreamRecord> convertedSchemas = converter.convert(allSchemas);

        StreamEntry outputEntry = new StreamEntry(
                stream.streamId(), job.getRecordType(), job.getTimestamp(), StreamEntry.EntryType.AGGREGATED);
        outputEntry.saveToS3(s3Client, finalBucketName, convertedSchemas);

        handleJobSuccess(job);

        logger.info(
                "Successfully processed job: listingId={}, schemaType={}, output={}",
                job.getListingId(),
                job.getRecordType(),
                outputEntry.getKey());
    }

    private List<StreamRecord> downloadSafely(StreamEntry entry) {
        try {
            return entry.loadFromS3(s3Client, mergedBucketName);
        } catch (NoSuchKeyException e) {
            logger.warn("No S3 key found for {}", entry);
            return List.of();
        }
    }

    private record Window(LocalDateTime start, LocalDateTime end) {}

    private Window calculateWindow(TransformationJob job) {
        return switch (job.getRecordType()) {
            case MBO, MBP_10, MBP_1, BBO_1S, BBO_1M, TRADES, OHLCV_1S, OHLCV_1M -> new Window(
                    job.getTimestamp(), job.getTimestamp());
            case OHLCV_1H -> new Window(
                    job.getTimestamp().withMinute(0), job.getTimestamp().withMinute(59));
        };
    }

    private void handleJobSuccess(TransformationJob job) {
        job.setStatus(TransformationStatus.COMPLETE);
        job.setProcessedAt(LocalDateTime.now(clock));
        job.setExpiresAt(LocalDateTime.now(clock).plusWeeks(1).toEpochSecond(ZoneOffset.UTC));

        transformJobsTable.updateItem(job);

        logger.info("Job marked as COMPLETE: listingId={}, schemaType={}", job.getListingId(), job.getRecordType());
    }

    private void handleJobFailure(TransformationJob job, String errorMessage) {
        job.setStatus(TransformationStatus.FAILED);
        job.setErrorMessage(errorMessage);
        job.setProcessedAt(LocalDateTime.now(clock));
        job.setExpiresAt(LocalDateTime.now(clock).plusWeeks(1).toEpochSecond(ZoneOffset.UTC));

        transformJobsTable.updateItem(job);

        logger.error(
                "Job marked as FAILED: listingId={}, schemaType={}, error={}",
                job.getListingId(),
                job.getRecordType(),
                errorMessage);
    }
}
