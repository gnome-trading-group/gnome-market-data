package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.S3Utils;
import group.gnometrading.SecurityMaster;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Set;
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
public final class JobCreatorLambdaHandler implements RequestHandler<SQSEvent, Void> {

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
    public Void handleRequest(SQSEvent event, Context context) {
        try {
            Set<MarketDataEntry> mergedKeys = S3Utils.extractKeysFromS3Event(event, objectMapper);
            logger.info("Found {} merged keys in S3 event", mergedKeys.size());

            for (MarketDataEntry entry : mergedKeys) {
                createTransformJobsForEntry(entry);
            }
        } catch (Exception e) {
            logger.error("Error processing messages: {}", e.getMessage());
            throw new RuntimeException("Failed to process messages", e);
        }
        return null;
    }

    private void createTransformJobsForEntry(MarketDataEntry entry) {
        for (SchemaType schemaType : SchemaType.values()) {
            if (SchemaConversionRegistry.hasBulkConverter(entry.getSchemaType(), schemaType)) {
                if (shouldCreateTransformJob(entry, schemaType)) {
                    createTransformJob(entry, schemaType);
                }
            }
        }
    }

    private boolean shouldCreateTransformJob(MarketDataEntry entry, SchemaType schemaType) {
        return switch (schemaType) {
            case MBO, MBP_10, MBP_1, BBO_1S, BBO_1M, TRADES, OHLCV_1S, OHLCV_1M -> true;
            case OHLCV_1H -> // Only create job at end of hour
            entry.getTimestamp().getMinute() == 59;
        };
    }

    private void createTransformJob(MarketDataEntry entry, SchemaType schemaType) {
        int listingId = this.securityMaster
                .getListing(entry.getExchangeId(), entry.getSecurityId())
                .listingId();

        TransformationJob job = new TransformationJob();
        job.setJobId(new JobId(listingId, schemaType));
        job.setTimestamp(entry.getTimestamp());
        job.setListingId(listingId);
        job.setSchemaType(schemaType);
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
                    schemaType.getIdentifier(),
                    entry.getTimestamp());
        } catch (ConditionalCheckFailedException e) {
            logger.info(
                    "Job already exists for listingId={}, schemaType={}, timestamp={}",
                    listingId,
                    schemaType.getIdentifier(),
                    entry.getTimestamp());
        }
    }
}
