package group.gnometrading;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.data.StreamEntry;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class S3Utils {

    private static final Logger logger = LogManager.getLogger(S3Utils.class);

    private S3Utils() {}

    /** The stream entries each S3 notification message names; a message that isn't one names none. */
    public static Set<StreamEntry> extractKeysFromS3Event(SQSEvent.SQSMessage message, ObjectMapper objectMapper) {
        JsonNode sqsBody;
        try {
            sqsBody = objectMapper.readTree(message.getBody());
        } catch (Exception e) {
            logger.error("Error parsing SQS message: {}", e.getMessage());
            return Set.of();
        }

        JsonNode snsMessage = sqsBody.get("Message");
        if (snsMessage == null) {
            logger.warn("No SNS Message field in SQS body");
            return Set.of();
        }

        JsonNode s3Event;
        try {
            String snsMessageString = snsMessage.asText();
            s3Event = objectMapper.readTree(snsMessageString);
        } catch (Exception e) {
            logger.error("Error parsing SNS message content: {}", e.getMessage());
            return Set.of();
        }

        JsonNode records = s3Event.get("Records");
        Set<StreamEntry> keys = new HashSet<>();
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                StreamEntry entry = parseS3Record(record);
                if (entry == null) {
                    continue;
                }
                keys.add(entry);
            }
        }
        return keys;
    }

    private static StreamEntry parseS3Record(JsonNode record) {
        JsonNode s3Info = record.get("s3");
        if (s3Info == null) {
            logger.warn("No S3 information in record");
            return null;
        }

        String bucketName = s3Info.get("bucket").get("name").asText();
        String objectKey = s3Info.get("object").get("key").asText();
        long objectSize = s3Info.get("object").get("size").asLong();

        logger.info("Processing S3 object: bucket={}, key={}, size={} bytes", bucketName, objectKey, objectSize);

        // A key in another layout (an old file, or anything else written to the bucket) is skipped rather than
        // failing the whole batch.
        StreamEntry entry = StreamEntry.parse(objectKey).orElse(null);
        if (entry == null) {
            logger.warn("Skipping key that is not a stream record file: {}", objectKey);
        }
        return entry;
    }
}
