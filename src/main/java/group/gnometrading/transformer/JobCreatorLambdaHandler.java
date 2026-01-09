package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.MarketDataEntry;
import group.gnometrading.S3Utils;
import group.gnometrading.SecurityMaster;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Lambda handler that creates transformation jobs for merged market data files.
 * Triggered by S3 events via SQS when new merged files are created.
 * Creates job entries in DynamoDB to be processed by JobProcessorLambdaHandler.
 */
public class JobCreatorLambdaHandler implements RequestHandler<SQSEvent, Void> {

    private final SecurityMaster securityMaster;
    private final ObjectMapper objectMapper;
    private final DynamoDbTable<TransformationJob> transformJobsTable;
    private final Clock clock;

    public JobCreatorLambdaHandler() {
        this(
                Dependencies.getInstance().getSecurityMaster(),
                Dependencies.getInstance().getObjectMapper(),
                Dependencies.getInstance().getTransformJobsTable(),
                Dependencies.getInstance().getClock()
        );
    }

    JobCreatorLambdaHandler(
            SecurityMaster securityMaster,
            ObjectMapper objectMapper,
            DynamoDbTable<TransformationJob> transformJobsTable,
            Clock clock
    ) {
        this.securityMaster = securityMaster;
        this.objectMapper = objectMapper;
        this.transformJobsTable = transformJobsTable;
        this.clock = clock;
    }
    
    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        try {
            Set<MarketDataEntry> mergedKeys = S3Utils.extractKeysFromS3Event(event, context, objectMapper);
            context.getLogger().log("Found " + mergedKeys.size() + " merged keys in S3 event");

            for (MarketDataEntry entry : mergedKeys) {
                createTransformJobsForEntry(entry, context);
            }
        } catch (Exception e) {
            context.getLogger().log("Error processing messages: " + e.getMessage());
            throw new RuntimeException("Failed to process messages", e);
        }
        return null;
    }

    private void createTransformJobsForEntry(MarketDataEntry entry, Context context) {
        context.getLogger().log("Creating transform jobs for entry: " + entry);

        for (SchemaType schemaType : SchemaType.values()) {
            if (entry.getSchemaType() == schemaType || SchemaConversionRegistry.hasConverter(entry.getSchemaType(), schemaType)) {
                createTransformJob(entry, schemaType, context);
            }
        }
    }

    private void createTransformJob(MarketDataEntry entry, SchemaType schemaType, Context context) {
        TransformationJob job = new TransformationJob();
        int listingId = this.securityMaster.getListing(entry.getExchangeId(), entry.getSecurityId()).listingId();
        job.setListingId(listingId);
        job.setSchemaType(schemaType.getIdentifier());
        job.setTimestamp(entry.getTimestamp());
        job.setStatus(TransformationStatus.PENDING);
        job.setCreatedAt(LocalDateTime.now(this.clock));


        try {
            this.transformJobsTable.putItem(builder -> builder
                    .item(job)
                    .conditionExpression(
                        Expression.builder()
                                .expression("attribute_not_exists(listingId) AND attribute_not_exists(schemaType)")
                                .build()
                    )
            );
        } catch (ConditionalCheckFailedException e) {
            context.getLogger().log("Job already exists for entry: " + entry + ", schema: " + schemaType);
        }
    }
}

