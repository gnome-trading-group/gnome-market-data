package group.gnometrading.merger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;

import com.fasterxml.jackson.databind.JsonNode;
import group.gnometrading.schemas.MBP10Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive test suite for MergerLambdaHandler.
 * Tests message parsing, S3 record extraction, and error handling.
 *
 * Note: This test suite focuses on unit testing the handler's logic without
 * actually performing S3 operations or merge operations, which are tested separately.
 */
@ExtendWith(MockitoExtension.class)
class MergerLambdaHandlerTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private S3Client s3Client;

    @Mock
    private Context context;

    @Mock
    private LambdaLogger logger;

    private MergerLambdaHandler handler;

    private static final String INPUT_BUCKET = "test-raw-bucket";
    private static final String OUTPUT_BUCKET = "test-merged-bucket";

    // List to store mocked S3 responses in order
    private List<byte[]> s3MockedDataList;
    private int s3GetObjectCallCount;

    @BeforeEach
    void setUp() {
        // Initialize handler with mocked dependencies
        handler = new MergerLambdaHandler(objectMapper, s3Client, INPUT_BUCKET, OUTPUT_BUCKET);

        // Setup context to return logger
        lenient().when(context.getLogger()).thenReturn(logger);

        // Initialize S3 mocked data list and counter
        s3MockedDataList = new ArrayList<>();
        s3GetObjectCallCount = 0;

        // Setup S3Client to return data from the list in order
        lenient().when(s3Client.getObject(any(java.util.function.Consumer.class)))
            .thenAnswer(invocation -> {
                if (s3GetObjectCallCount >= s3MockedDataList.size()) {
                    throw new RuntimeException("No more mocked S3 data available");
                }
                byte[] data = s3MockedDataList.get(s3GetObjectCallCount++);
                InputStream inputStream = new ByteArrayInputStream(data);
                return new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    AbortableInputStream.create(inputStream)
                );
            });
    }

    // ============================================================================
    // HANDLE REQUEST TESTS
    // ============================================================================

    @Test
    void testHandleRequestWithEmptyEvent() {
        // Given: Empty SQS event
        SQSEvent event = new SQSEvent();
        event.setRecords(new ArrayList<>());

        // When: Handling request
        Void result = handler.handleRequest(event, context);

        // Then: Returns null and logs message count
        assertNull(result);
        verify(logger).log(contains("Received SQS event with 0 messages"));
    }

    @Test
    void testHandleRequestWithNullRecords() {
        // Given: SQS event with null records
        SQSEvent event = new SQSEvent();
        event.setRecords(null);

        // When/Then: Should throw NullPointerException
        assertThrows(NullPointerException.class, () -> handler.handleRequest(event, context));
    }

    // ============================================================================
    // ERROR HANDLING TESTS
    // ============================================================================

    @Test
    void testHandleRequestWithMalformedJson() throws Exception {
        // Given: SQS event with malformed JSON
        String malformedJson = createMalformedJson();
        SQSEvent event = createSQSEvent(malformedJson);

        when(objectMapper.readTree(malformedJson))
            .thenThrow(new RuntimeException("Malformed JSON"));

        // When: Handling request (error is caught and logged, not thrown)
        Void result = handler.handleRequest(event, context);

        // Then: Should handle gracefully and log error
        assertNull(result);
        verify(logger).log(contains("Error parsing message"));
    }

    @Test
    void testHandleRequestWithNoRecordsInS3Event() throws Exception {
        // Given: S3 event with no Records array
        String eventJson = createS3EventJsonNoRecords();
        SQSEvent event = createSQSEvent(eventJson);

        when(objectMapper.readTree(eventJson)).thenReturn(
            new ObjectMapper().readTree(eventJson)
        );

        // When: Handling request
        Void result = handler.handleRequest(event, context);

        // Then: Returns null without errors (empty map, no processing)
        assertNull(result);
        verify(logger).log(contains("Processing message"));
    }

    @Test
    void testHandleRequestWithMissingS3Fields() throws Exception {
        // Given: S3 event with missing required fields
        String eventJson = createS3EventJsonMissingFields();
        SQSEvent event = createSQSEvent(eventJson);

        when(objectMapper.readTree(eventJson)).thenReturn(
            new ObjectMapper().readTree(eventJson)
        );

        // When: Handling request (missing fields are handled gracefully)
        Void result = handler.handleRequest(event, context);

        // Then: Should handle gracefully and log
        assertNull(result);
        verify(logger).log(contains("No S3 information in record"));
    }

    @Test
    void testHandleRequestWithNullS3Info() throws Exception {
        // Given: S3 event record with null s3 field
        String eventJson = """
            {
              "Records": [
                {
                  "eventVersion": "2.1",
                  "eventSource": "aws:s3"
                }
              ]
            }
            """;
        SQSEvent event = createSQSEvent(eventJson);

        when(objectMapper.readTree(eventJson)).thenReturn(
            new ObjectMapper().readTree(eventJson)
        );

        // When: Handling request
        Void result = handler.handleRequest(event, context);

        // Then: Should handle gracefully and log (returns empty map, no processing)
        assertNull(result);
        verify(logger).log(contains("No S3 information in record"));
    }

    // ============================================================================
    // PARAMETERIZED TESTS FOR ERROR SCENARIOS
    // ============================================================================

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidS3EventTestCases")
    void testHandleRequestWithInvalidS3Events(String description, String eventJson, boolean shouldThrow) throws Exception {
        // Given: Various invalid S3 event formats
        SQSEvent event = createSQSEvent(eventJson);

        when(objectMapper.readTree(eventJson)).thenReturn(
            new ObjectMapper().readTree(eventJson)
        );

        // When/Then: Verify behavior
        if (shouldThrow) {
            assertThrows(RuntimeException.class, () -> handler.handleRequest(event, context));
        } else {
            Void result = handler.handleRequest(event, context);
            assertNull(result);
        }
    }

    static Stream<Arguments> invalidS3EventTestCases() {
        return Stream.of(
            Arguments.of("Empty JSON object", "{}", false),
            Arguments.of("Records is null", "{\"Records\": null}", false),
            Arguments.of("Records is empty array", "{\"Records\": []}", false),
            Arguments.of("Record with no s3 field",
                "{\"Records\": [{\"eventVersion\": \"2.1\"}]}", false)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("jsonParsingErrorTestCases")
    void testHandleRequestWithJsonParsingErrors(String description, String eventJson) throws Exception {
        // Given: Event JSON that causes parsing errors
        SQSEvent event = createSQSEvent(eventJson);

        when(objectMapper.readTree(eventJson))
            .thenThrow(new RuntimeException("JSON parsing error"));

        // When: Handling request (error is caught and logged, not thrown)
        Void result = handler.handleRequest(event, context);

        // Then: Should handle gracefully and log error
        assertNull(result);
        verify(logger).log(contains("Error parsing message"));
    }

    static Stream<Arguments> jsonParsingErrorTestCases() {
        return Stream.of(
            Arguments.of("Malformed JSON", "{\"Records\": [invalid}"),
            Arguments.of("Incomplete JSON", "{\"Records\":"),
            Arguments.of("Invalid characters", "{\"Records\": [\u0000]}")
        );
    }

    // ============================================================================
    // INTEGRATION-STYLE TESTS (with minimal mocking)
    // ============================================================================

    @Test
    void testHandleRequestLogsCorrectMessageCount() throws Exception {
        // Given: SQS event with multiple messages
        SQSEvent event = createSQSEvent("msg1", "msg2", "msg3");

        // Setup objectMapper to return empty JSON for all messages
        lenient().when(objectMapper.readTree(anyString())).thenAnswer(invocation -> {
            return new ObjectMapper().readTree("{}");
        });

        // When: Handling request
        Void result = handler.handleRequest(event, context);

        // Then: Should log correct message count
        assertNull(result);
        verify(logger).log(contains("Received SQS event with 3 messages"));
        verify(logger, times(3)).log(contains("Processing message"));
    }

    @Test
    void testHandleRequestProcessesEachMessage() throws Exception {
        // Given: SQS event with 2 messages
        String msg1 = "{}";
        String msg2 = "{}";
        SQSEvent event = createSQSEvent(msg1, msg2);

        when(objectMapper.readTree(anyString())).thenReturn(
            new ObjectMapper().readTree("{}")
        );

        // When: Handling request
        Void result = handler.handleRequest(event, context);

        // Then: Should process each message
        assertNull(result);
        verify(objectMapper, times(2)).readTree(anyString());
        verify(logger, times(2)).log(contains("Processing message"));
    }

    // ============================================================================
    // BUSINESS LOGIC TESTS - S3 Listing and Merging
    // ============================================================================

    /**
     * Tests that when a notification arrives for a single raw file,
     * and it's the only file in S3, it gets merged by itself.
     */
    @Test
    void testNotificationForSingleFileInS3() throws Exception {
        // Given: Notification for uuid-1, and S3 listing returns only uuid-1
        String rawKey = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String s3EventJson = createS3EventJsonWithKey(rawKey);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 listing to return only the notified file
        mockS3ListObjects("1/2/2024/1/15/10/0/mbp-10/", List.of(rawKey));

        // Mock S3 getObject for the file
        mockS3GetObject(rawKey, List.of(createSchema(100L), createSchema(101L)));

        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should list S3, load 1 file, and merge
        verify(s3Client).listObjectsV2Paginator(any(java.util.function.Consumer.class));
        verify(s3Client, times(1)).getObject(any(java.util.function.Consumer.class));
        verify(s3Client, times(1)).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
        verify(logger).log(contains("Merging 1 entries"));
        verify(logger).log(contains("Wrote 2"));
    }

    /**
     * Tests that when a notification arrives for one file,
     * but S3 listing finds multiple files, all are merged together.
     */
    @Test
    void testNotificationForOneFileButMultipleFilesExistInS3() throws Exception {
        // Given: Notification for uuid-1, but S3 has uuid-1, uuid-2, uuid-3
        String notifiedKey = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String key2 = "1/2/2024/1/15/10/0/mbp-10/uuid-2.zst";
        String key3 = "1/2/2024/1/15/10/0/mbp-10/uuid-3.zst";

        String s3EventJson = createS3EventJsonWithKey(notifiedKey);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 listing to return all 3 files
        mockS3ListObjects("1/2/2024/1/15/10/0/mbp-10/", List.of(notifiedKey, key2, key3));

        // Mock S3 getObject for all files
        mockS3GetObject(notifiedKey, List.of(createSchema(100L), createSchema(101L), createSchema(102L)));
        mockS3GetObject(key2, List.of(createSchema(100L), createSchema(101L)));
        mockS3GetObject(key3, List.of(createSchema(100L), createSchema(101L), createSchema(102L)));

        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should list S3, load all 3 files, and merge them
        verify(s3Client).listObjectsV2Paginator(any(java.util.function.Consumer.class));
        verify(s3Client, times(3)).getObject(any(java.util.function.Consumer.class));
        verify(s3Client, times(1)).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
        verify(logger).log(contains("Merging 3 entries"));
        verify(logger).log(contains("Wrote 3")); // uuid-1 or uuid-3 wins with 3 records
    }

    /**
     * Tests that notifications for different timestamps create separate merged files.
     */
    @Test
    void testNotificationsForDifferentTimestampsCreateSeparateMerges() throws Exception {
        // Given: Notifications for files at 10:00 and 11:00
        String key1 = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String key2 = "1/2/2024/1/15/11/0/mbp-10/uuid-2.zst";

        String s3EventJson = createS3EventJsonWithMultipleKeys(key1, key2);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 listing for each timestamp
        mockS3ListObjects("1/2/2024/1/15/10/0/mbp-10/", List.of(key1));
        mockS3ListObjects("1/2/2024/1/15/11/0/mbp-10/", List.of(key2));

        // Mock S3 getObject for each file
        mockS3GetObject(key1, List.of(createSchema(100L)));
        mockS3GetObject(key2, List.of(createSchema(200L)));

        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should list S3 twice (once per timestamp), load 2 files, create 2 merged files
        verify(s3Client, times(2)).listObjectsV2Paginator(any(java.util.function.Consumer.class));
        verify(s3Client, times(2)).getObject(any(java.util.function.Consumer.class));
        verify(s3Client, times(2)).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
    }

    /**
     * Tests that duplicate notifications for the same timestamp are merged together.
     * Multiple SQS messages with notifications for files at the same timestamp
     * will each query S3 (one per message), but the results are deduplicated
     * and merged into a single output file.
     */
    @Test
    void testDuplicateNotificationsForSameTimestampAreMergedTogether() throws Exception {
        // Given: Two SQS messages with notifications for files at the same timestamp
        String key1 = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String key2 = "1/2/2024/1/15/10/0/mbp-10/uuid-2.zst";

        String sqsMessage1Json = createS3EventJsonWithKey(key1);
        String sqsMessage2Json = createS3EventJsonWithKey(key2);

        SQSEvent event = new SQSEvent();
        SQSEvent.SQSMessage message1 = new SQSEvent.SQSMessage();
        message1.setBody(sqsMessage1Json);
        SQSEvent.SQSMessage message2 = new SQSEvent.SQSMessage();
        message2.setBody(sqsMessage2Json);
        event.setRecords(List.of(message1, message2));

        when(objectMapper.readTree(sqsMessage1Json)).thenReturn(
            new ObjectMapper().readTree(sqsMessage1Json)
        );
        when(objectMapper.readTree(sqsMessage2Json)).thenReturn(
            new ObjectMapper().readTree(sqsMessage2Json)
        );

        // Mock S3 listing to return both files (they're both in S3)
        // Each SQS message will trigger a separate S3 listing call
        mockS3ListObjects("1/2/2024/1/15/10/0/mbp-10/", List.of(key1, key2));

        // Mock S3 getObject for both files
        mockS3GetObject(key1, List.of(createSchema(100L), createSchema(101L)));
        mockS3GetObject(key2, List.of(createSchema(100L), createSchema(101L), createSchema(102L)));

        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should list S3 twice (once per SQS message), but deduplicate and load each file once
        verify(s3Client, times(2)).listObjectsV2Paginator(any(java.util.function.Consumer.class));
        verify(s3Client, times(2)).getObject(any(java.util.function.Consumer.class));
        verify(s3Client, times(1)).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
        verify(logger).log(contains("Merging 2 entries"));
        verify(logger).log(contains("Wrote 3")); // uuid-2 wins with 3 records
    }

    /**
     * Tests edge case: S3 listing returns empty (no files found).
     * This could happen if files were deleted between notification and processing.
     * The handler will still attempt to merge with 0 entries.
     */
    @Test
    void testS3ListingReturnsNoFiles() throws Exception {
        // Given: Notification for a file, but S3 listing returns empty
        String rawKey = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String s3EventJson = createS3EventJsonWithKey(rawKey);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 listing to return empty list
        mockS3ListObjects("1/2/2024/1/15/10/0/mbp-10/", List.of());

        // Mock putObject since the handler will still try to save an empty merge
        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should list S3, and merge 0 entries (creating an empty output)
        verify(s3Client).listObjectsV2Paginator(any(java.util.function.Consumer.class));
        verify(s3Client, never()).getObject(any(java.util.function.Consumer.class));
        verify(s3Client, times(1)).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
        verify(logger).log(contains("Merging 0 entries"));
        verify(logger).log(contains("Wrote 0"));
    }

    /**
     * Tests that the Set properly deduplicates when S3 listing returns the same file
     * that was in the notification.
     */
    @Test
    void testSetDeduplicatesFilesFromListingAndNotification() throws Exception {
        // Given: Notification for uuid-1, and S3 listing also returns uuid-1
        String rawKey = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String s3EventJson = createS3EventJsonWithKey(rawKey);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 listing to return the same file (uuid-1)
        mockS3ListObjects("1/2/2024/1/15/10/0/mbp-10/", List.of(rawKey));

        // Mock S3 getObject for the file
        mockS3GetObject(rawKey, List.of(createSchema(100L), createSchema(101L)));

        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should only load the file once (deduplicated by Set)
        verify(s3Client).listObjectsV2Paginator(any(java.util.function.Consumer.class));
        verify(s3Client, times(1)).getObject(any(java.util.function.Consumer.class));
        verify(s3Client, times(1)).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
        verify(logger).log(contains("Merging 1 entries"));
    }

    /**
     * Tests that S3 listing pagination works correctly.
     * When S3 returns multiple pages of results, all files should be merged.
     */
    @Test
    void testS3ListingWithMultiplePages() throws Exception {
        // Given: Notification for a file, and S3 listing returns many files (simulating pagination)
        String notifiedKey = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        List<String> allKeys = new ArrayList<>();
        allKeys.add(notifiedKey);
        // Add 9 more files to simulate a large listing
        for (int i = 2; i <= 10; i++) {
            allKeys.add("1/2/2024/1/15/10/0/mbp-10/uuid-" + i + ".zst");
        }

        String s3EventJson = createS3EventJsonWithKey(notifiedKey);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 listing to return all 10 files
        mockS3ListObjects("1/2/2024/1/15/10/0/mbp-10/", allKeys);

        // Mock S3 getObject for all files
        for (String key : allKeys) {
            mockS3GetObject(key, List.of(createSchema(100L)));
        }

        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should list S3, load all 10 files, and merge them
        verify(s3Client).listObjectsV2Paginator(any(java.util.function.Consumer.class));
        verify(s3Client, times(10)).getObject(any(java.util.function.Consumer.class));
        verify(s3Client, times(1)).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
        verify(logger).log(contains("Merging 10 entries"));
    }

    /**
     * Tests that when S3 getObject fails for one file, the error is logged
     * and the handler throws an exception.
     */
    @Test
    void testS3GetObjectFailure() throws Exception {
        // Given: Notification for a file, S3 listing succeeds, but getObject fails
        String rawKey = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String s3EventJson = createS3EventJsonWithKey(rawKey);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 listing to return the file
        mockS3ListObjects("1/2/2024/1/15/10/0/mbp-10/", List.of(rawKey));

        // Mock S3 getObject to throw an exception
        when(s3Client.getObject(any(java.util.function.Consumer.class)))
            .thenThrow(new RuntimeException("S3 getObject failed"));

        // When/Then: Handling the request should throw an exception
        try {
            handler.handleRequest(event, context);
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            // Expected
            verify(s3Client).listObjectsV2Paginator(any(java.util.function.Consumer.class));
            verify(s3Client).getObject(any(java.util.function.Consumer.class));
            verify(logger).log(contains("Error processing messages"));
        }
    }

    // ============================================================================
    // HELPER METHODS
    // ============================================================================

    /**
     * Creates a sample SQS event with the given JSON body.
     */
    private SQSEvent createSQSEvent(String... messageBodies) {
        SQSEvent event = new SQSEvent();
        List<SQSEvent.SQSMessage> messages = new ArrayList<>();

        for (String body : messageBodies) {
            messages.add(createSQSMessage(body));
        }

        event.setRecords(messages);
        return event;
    }

    /**
     * Creates a sample SQS message with the given body.
     */
    private SQSEvent.SQSMessage createSQSMessage(String body) {
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setBody(body);
        message.setMessageId("test-message-id-" + System.nanoTime());
        return message;
    }

    /**
     * Creates malformed JSON for testing error handling.
     */
    private String createMalformedJson() {
        return "{\"Records\": [invalid json}";
    }

    /**
     * Creates S3 event JSON with missing required fields.
     */
    private String createS3EventJsonMissingFields() {
        return """
            {
              "Records": [
                {
                  "eventVersion": "2.1",
                  "eventSource": "aws:s3"
                }
              ]
            }
            """;
    }

    /**
     * Creates S3 event JSON with no Records array.
     */
    private String createS3EventJsonNoRecords() {
        return "{}";
    }

    /**
     * Creates S3 event JSON with a specific S3 object key.
     */
    private String createS3EventJsonWithKey(String key) {
        return String.format("""
            {
              "Records": [
                {
                  "eventVersion": "2.1",
                  "eventSource": "aws:s3",
                  "eventName": "ObjectCreated:Put",
                  "s3": {
                    "bucket": {
                      "name": "%s"
                    },
                    "object": {
                      "key": "%s",
                      "size": 1024
                    }
                  }
                }
              ]
            }
            """, INPUT_BUCKET, key);
    }

    /**
     * Creates S3 event JSON with multiple S3 object keys.
     */
    private String createS3EventJsonWithMultipleKeys(String... keys) {
        StringBuilder records = new StringBuilder();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) records.append(",");
            records.append(String.format("""
                {
                  "eventVersion": "2.1",
                  "eventSource": "aws:s3",
                  "eventName": "ObjectCreated:Put",
                  "s3": {
                    "bucket": {
                      "name": "%s"
                    },
                    "object": {
                      "key": "%s",
                      "size": 1024
                    }
                  }
                }
                """, INPUT_BUCKET, keys[i]));
        }
        return String.format("{\"Records\": [%s]}", records.toString());
    }

    /**
     * Creates a Schema with a specific sequence number.
     * Helper method similar to MBP10MergeStrategyTest.
     */
    private Schema createSchema(long sequenceNumber) {
        MBP10Schema schema = (MBP10Schema) SchemaType.MBP_10.newInstance();
        schema.encoder.sequence(sequenceNumber);
        return schema;
    }

    /**
     * Mocks S3Client.listObjectsV2Paginator to return a list of keys with the given prefix.
     */
    private void mockS3ListObjects(String prefix, List<String> keys) {
        // Create S3Object instances for each key
        List<S3Object> s3Objects = keys.stream()
            .map(key -> S3Object.builder().key(key).build())
            .toList();

        // Create a mock iterable
        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);

        // Mock the contents() method to return a stream of S3Objects
        // We need to return a new stream each time to avoid stream reuse issues
        when(mockIterable.contents()).thenAnswer(invocation -> {
            // Create a mock SdkIterable
            software.amazon.awssdk.core.pagination.sync.SdkIterable<S3Object> mockSdkIterable =
                mock(software.amazon.awssdk.core.pagination.sync.SdkIterable.class);
            when(mockSdkIterable.stream()).thenReturn(s3Objects.stream());
            return mockSdkIterable;
        });

        // Mock the S3Client to return the iterable when listObjectsV2Paginator is called
        // We use lenient() because not all tests will call this
        lenient().when(s3Client.listObjectsV2Paginator(argThat((java.util.function.Consumer<ListObjectsV2Request.Builder> consumer) -> {
            if (consumer == null) return false;
            ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder();
            consumer.accept(builder);
            ListObjectsV2Request request = builder.build();
            return request.prefix() != null && request.prefix().equals(prefix);
        }))).thenReturn(mockIterable);
    }

    /**
     * Mocks S3Client.getObject to return compressed schema data.
     * Adds the data to the list so it will be returned in order on subsequent getObject calls.
     */
    private void mockS3GetObject(String key, List<Schema> schemas) throws IOException {
        // Serialize schemas to bytes
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Schema schema : schemas) {
            byte[] schemaBytes = new byte[schema.totalMessageSize()];
            schema.buffer.getBytes(0, schemaBytes, 0, schema.totalMessageSize());
            outputStream.write(schemaBytes);
        }
        byte[] uncompressedData = outputStream.toByteArray();

        // Compress with Zstd
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdStream = new ZstdOutputStream(compressedOutput)) {
            zstdStream.write(uncompressedData);
        }
        byte[] compressedData = compressedOutput.toByteArray();

        // Add to the list so it will be returned in order
        s3MockedDataList.add(compressedData);
    }
}
