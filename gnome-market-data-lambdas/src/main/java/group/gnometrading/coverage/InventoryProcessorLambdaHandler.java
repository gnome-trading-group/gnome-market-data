package group.gnometrading.coverage;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Lambda handler for processing S3 inventory Parquet files.
 * Triggered by SQS messages from S3 event notifications.
 */
public final class InventoryProcessorLambdaHandler implements RequestHandler<SQSEvent, Void> {

    private static final Logger logger = LogManager.getLogger(InventoryProcessorLambdaHandler.class);

    private final InventoryProcessor processor;
    private final ObjectMapper objectMapper;

    public InventoryProcessorLambdaHandler() {
        this(
                new InventoryProcessor(
                        Dependencies.getInstance().getS3Client(),
                        Dependencies.getInstance().getDynamoDbEnhancedClient(),
                        Dependencies.getInstance().getCoverageTable(),
                        Dependencies.getInstance().getMetadataBucketName()),
                Dependencies.getInstance().getObjectMapper());
    }

    public InventoryProcessorLambdaHandler(InventoryProcessor processor, ObjectMapper objectMapper) {
        this.processor = processor;
        this.objectMapper = objectMapper;
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        logger.info("Received SQS event with {} messages", event.getRecords().size());

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                String inventoryKey = extractS3KeyFromMessage(message);

                if (inventoryKey == null) {
                    logger.warn("Could not extract S3 key from message, skipping");
                    continue;
                }

                processor.processInventoryFile(inventoryKey);
            } catch (Exception e) {
                logger.error("Error processing message: {}", e.getMessage());
                throw new RuntimeException("Failed to process inventory file", e);
            }
        }
        return null;
    }

    private String extractS3KeyFromMessage(SQSEvent.SQSMessage message) throws JsonProcessingException {
        String body = message.getBody();
        JsonNode root = objectMapper.readTree(body);

        // S3 event notification structure:
        // { "Records": [ { "s3": { "object": { "key": "..." } } } ] }
        JsonNode records = root.get("Records");
        if (records == null || !records.isArray() || records.isEmpty()) {
            logger.warn("No Records found in message body");
            return null;
        }

        JsonNode firstRecord = records.get(0);
        JsonNode s3Info = firstRecord.get("s3");
        if (s3Info == null) {
            logger.warn("No s3 info in record");
            return null;
        }

        JsonNode objectInfo = s3Info.get("object");
        if (objectInfo == null) {
            logger.warn("No object info in s3 record");
            return null;
        }

        JsonNode keyNode = objectInfo.get("key");
        if (keyNode == null) {
            logger.warn("No key in object info");
            return null;
        }

        String key = keyNode.asText();
        logger.info("Extracted S3 key: {}", key);
        return key;
    }
}
