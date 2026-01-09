package group.gnometrading;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class S3Utils {

    public static Set<MarketDataEntry> extractKeysFromS3Event(SQSEvent event, Context context, ObjectMapper objectMapper) {
        context.getLogger().log("Extracting keys from SQS event with " + event.getRecords().size() + " messages");
        return event.getRecords().stream()
                .map(message -> extractKeysFromS3Event(message, context, objectMapper))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    private static Set<MarketDataEntry> extractKeysFromS3Event(SQSEvent.SQSMessage message, Context context, ObjectMapper objectMapper) {
        JsonNode sqsBody;
        try {
            sqsBody = objectMapper.readTree(message.getBody());
        } catch (Exception e) {
            context.getLogger().log("Error parsing SQS message: " + e.getMessage());
            return Set.of();
        }

        JsonNode snsMessage = sqsBody.get("Message");
        if (snsMessage == null) {
            context.getLogger().log("No SNS Message field in SQS body");
            return Set.of();
        }

        JsonNode s3Event;
        try {
            String snsMessageString = snsMessage.asText();
            s3Event = objectMapper.readTree(snsMessageString);
        } catch (Exception e) {
            context.getLogger().log("Error parsing SNS message content: " + e.getMessage());
            return Set.of();
        }

        JsonNode records = s3Event.get("Records");
        Set<MarketDataEntry> keys = new HashSet<>();
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                MarketDataEntry entry = parseS3Record(record, context);
                if (entry == null) {
                    continue;
                }
                keys.add(entry);
            }
        }
        return keys;
    }

    private static MarketDataEntry parseS3Record(JsonNode record, Context context) {
        JsonNode s3Info = record.get("s3");
        if (s3Info == null) {
            context.getLogger().log("No S3 information in record");
            return null;
        }

        String bucketName = s3Info.get("bucket").get("name").asText();
        String objectKey = s3Info.get("object").get("key").asText();
        long objectSize = s3Info.get("object").get("size").asLong();

        context.getLogger().log(String.format(
            "Processing S3 object: bucket=%s, key=%s, size=%d bytes",
            bucketName, objectKey, objectSize
        ));

        return MarketDataEntry.fromKey(objectKey);
    }
}
