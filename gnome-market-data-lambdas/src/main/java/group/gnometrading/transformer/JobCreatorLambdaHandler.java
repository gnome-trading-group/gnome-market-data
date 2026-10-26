package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.SecurityMaster;
import group.gnometrading.SqsBatch;
import group.gnometrading.data.StreamEntry;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import java.time.Clock;
import java.time.LocalDateTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

/**
 * Lambda handler that creates transformation jobs for merged market data files.
 * Triggered by S3 events via SQS when new merged files are created.
 * Creates job entries in DynamoDB to be processed by JobProcessorLambdaHandler.
 */
public final class JobCreatorLambdaHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {

    private static final Logger logger = LogManager.getLogger(JobCreatorLambdaHandler.class);

    private final SecurityMaster securityMaster;
    private final ObjectMapper objectMapper;
    private final DynamoDbTable<TransformationJob> transformJobsTable;
    private final Clock clock;

    public JobCreatorLambdaHandler() {
        this(
                Dependencies.getInstance().getSecurityMaster(),
                Dependencies.getInstance().getObjectMapper(),
                Dependencies.getInstance().getTransformJobsTable(),
                Dependencies.getInstance().getClock());
    }

    JobCreatorLambdaHandler(
            SecurityMaster securityMaster,
            ObjectMapper objectMapper,
            DynamoDbTable<TransformationJob> transformJobsTable,
            Clock clock) {
        this.securityMaster = securityMaster;
        this.objectMapper = objectMapper;
        this.transformJobsTable = transformJobsTable;
        this.clock = clock;
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        return SqsBatch.forEachUnit(event, objectMapper, entry -> entry, false, this::createTransformJobsForEntry);
    }

    private void createTransformJobsForEntry(StreamEntry entry) {
        for (RecordType schemaType : RecordType.values()) {
            if (SchemaConversionRegistry.hasBulkConverter(entry.getRecordType(), schemaType)) {
                if (shouldCreateTransformJob(entry, schemaType)) {
                    createTransformJob(entry, schemaType);
                }
            }
        }
    }

    private boolean shouldCreateTransformJob(StreamEntry entry, RecordType schemaType) {
        return switch (schemaType) {
            case MBO, MBP_10, MBP_1, BBO_1S, BBO_1M, TRADES, OHLCV_1S, OHLCV_1M -> true;
            case OHLCV_1H -> // Only create job at end of hour
            entry.getTimestamp().getMinute() == 59;
        };
    }

    private void createTransformJob(StreamEntry entry, RecordType schemaType) {
        int listingId =
                this.securityMaster.listingForStream(entry.getStreamId()).listingId();

        TransformationJob job = new TransformationJob();
        job.setJobId(new JobId(listingId, schemaType));
        job.setTimestamp(entry.getTimestamp());
        job.setListingId(listingId);
        job.setRecordType(schemaType);
        job.setSourceRecordType(entry.getRecordType());
        job.setStatus(TransformationStatus.PENDING);
        job.setCreatedAt(LocalDateTime.now(this.clock));

        try {
            this.transformJobsTable.putItem(request -> request.item(job)
                    .conditionExpression(Expression.builder()
                            .expression("attribute_not_exists(jobId) AND attribute_not_exists(#ts)")
                            .putExpressionName("#ts", "timestamp")
                            .build()));
            logger.info(
                    "Created transformation job: listingId={}, schemaType={}, timestamp={}",
                    listingId,
                    schemaType.identifier(),
                    entry.getTimestamp());
        } catch (ConditionalCheckFailedException e) {
            logger.info(
                    "Job already exists for listingId={}, schemaType={}, timestamp={}",
                    listingId,
                    schemaType.identifier(),
                    entry.getTimestamp());
        }
    }
}
