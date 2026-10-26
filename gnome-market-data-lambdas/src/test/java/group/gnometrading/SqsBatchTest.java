package group.gnometrading;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.data.StreamEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SqsBatchTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private SQSEvent.SQSMessage message(String id, String... keys) throws Exception {
        List<Map<String, Object>> records = new ArrayList<>();
        for (String key : keys) {
            records.add(
                    Map.of("s3", Map.of("bucket", Map.of("name", "bucket"), "object", Map.of("key", key, "size", 1))));
        }
        String s3Event = objectMapper.writeValueAsString(Map.of("Records", records));
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setMessageId(id);
        message.setBody(objectMapper.writeValueAsString(Map.of("Message", s3Event)));
        return message;
    }

    private static Set<String> failed(SQSBatchResponse response) {
        return response.getBatchItemFailures().stream()
                .map(SQSBatchResponse.BatchItemFailure::getItemIdentifier)
                .collect(Collectors.toSet());
    }

    @Test
    void aFailedUnitFailsEveryMessageBehindItAndNoOther() throws Exception {
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(
                message("a", "1/book/mbp-10/2026/01/01/00/00/x.zst"),
                message("b", "1/book/mbp-10/2026/01/01/00/00/y.zst"),
                message("c", "2/book/mbp-10/2026/01/01/00/00/z.zst")));

        SQSBatchResponse response =
                SqsBatch.forEachUnit(event, objectMapper, StreamEntry::getStreamId, false, stream -> {
                    if (stream == 1) {
                        throw new IllegalStateException("bad minute");
                    }
                });

        assertEquals(Set.of("a", "b"), failed(response));
    }

    @Test
    void aMessageThatIsNotAnS3NotificationFailsNothing() {
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setMessageId("junk");
        message.setBody("{not json");
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(message));

        assertTrue(SqsBatch.forEachUnit(event, objectMapper, entry -> entry, true, entry -> {})
                .getBatchItemFailures()
                .isEmpty());
    }
}
