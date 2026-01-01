package group.gnometrading.merger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

/**
 * Lambda handler for processing S3 object creation events from SQS queue.
 * This handler receives S3 event notifications via SQS and processes raw market data files.
 */
public class MergerLambdaHandler implements RequestHandler<SQSEvent, Void> {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Received SQS event with " + event.getRecords().size() + " messages");
        
        for (SQSMessage message : event.getRecords()) {
            try {
                processMessage(message, context);
            } catch (Exception e) {
                context.getLogger().log("Error processing message: " + e.getMessage());
                // Re-throw to mark the message as failed and allow retry
                throw new RuntimeException("Failed to process message", e);
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
    
    private void processS3Record(JsonNode record, Context context) {
        // Extract S3 bucket and key information
        JsonNode s3Info = record.get("s3");
        if (s3Info == null) {
            context.getLogger().log("No S3 information in record");
            return;
        }
        
        String bucketName = s3Info.get("bucket").get("name").asText();
        String objectKey = s3Info.get("object").get("key").asText();
        long objectSize = s3Info.get("object").get("size").asLong();
        
        context.getLogger().log(String.format(
            "Processing S3 object: bucket=%s, key=%s, size=%d bytes",
            bucketName, objectKey, objectSize
        ));
        
        // TODO: Implement your business logic here
        // This is where you would:
        // 1. Download the file from S3 (rawBucket)
        // 2. Process/merge the market data
        // 3. Upload the result to the mergedBucket
        // 4. Update any necessary metadata in DynamoDB
        
        context.getLogger().log("Successfully processed object: " + objectKey);
    }
}

