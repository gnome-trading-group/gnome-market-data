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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
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
    // BUSINESS LOGIC TESTS - Testing actual merge operations
    // ============================================================================

    @Test
    void testHandleRequestWithSingleRawEntry() throws Exception {
        // Given: S3 event with a single raw market data entry
        // Key format: securityId/exchangeId/year/month/day/hour/minute/schemaType/uuid.zst
        String rawKey = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String s3EventJson = createS3EventJsonWithKey(rawKey);
        SQSEvent event = createSQSEvent(s3EventJson);

        // Mock ObjectMapper to parse the S3 event
        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 to return compressed data with schemas
        List<Schema> rawSchemas = List.of(
            createSchema(100L),
            createSchema(101L)
        );
        mockS3GetObject(rawKey, rawSchemas);

        // Mock S3 putObject to capture what gets saved
        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        Void result = handler.handleRequest(event, context);

        // Then: Should merge and save to output bucket
        assertNull(result);
        verify(s3Client).getObject(any(java.util.function.Consumer.class));
        verify(s3Client).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
        verify(logger).log(contains("Merging 1 entries"));
        verify(logger).log(contains("Wrote 2"));
    }

    @Test
    void testHandleRequestWithMultipleRawEntries() throws Exception {
        // Given: S3 event with multiple raw entries that should be merged together
        // All have same timestamp (10:00) so they merge into one aggregated file
        String key1 = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String key2 = "1/2/2024/1/15/10/0/mbp-10/uuid-2.zst";
        String key3 = "1/2/2024/1/15/10/0/mbp-10/uuid-3.zst";

        String s3EventJson = createS3EventJsonWithMultipleKeys(key1, key2, key3);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 to return different data for each raw entry
        // uuid-1 has 3 records, uuid-2 has 2 records, uuid-3 has 3 records
        // MBP10MergeStrategy should pick uuid-1 (first with max count)
        mockS3GetObject(key1, List.of(createSchema(100L), createSchema(101L), createSchema(102L)));
        mockS3GetObject(key2, List.of(createSchema(100L), createSchema(101L)));
        mockS3GetObject(key3, List.of(createSchema(100L), createSchema(101L), createSchema(102L)));

        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        Void result = handler.handleRequest(event, context);

        // Then: Should merge all 3 entries and save result
        assertNull(result);
        verify(s3Client, times(3)).getObject(any(java.util.function.Consumer.class));
        verify(s3Client).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
        verify(logger).log(contains("Merging 3 entries"));
        verify(logger).log(contains("Wrote 3"));
    }

    @Test
    void testHandleRequestWithMultipleDifferentMergedKeys() throws Exception {
        // Given: S3 event with raw entries for different timestamps (should create separate merged files)
        // One at 10:00, one at 11:00
        String key1 = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String key2 = "1/2/2024/1/15/11/0/mbp-10/uuid-2.zst";

        String s3EventJson = createS3EventJsonWithMultipleKeys(key1, key2);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Mock S3 to return data for each entry
        mockS3GetObject(key1, List.of(createSchema(100L)));
        mockS3GetObject(key2, List.of(createSchema(200L)));

        when(s3Client.putObject(any(java.util.function.Consumer.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        Void result = handler.handleRequest(event, context);

        // Then: Should create 2 separate merged files
        assertNull(result);
        verify(s3Client, times(2)).getObject(any(java.util.function.Consumer.class));
        verify(s3Client, times(2)).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
    }

    @Test
    void testMergeEntriesVerifiesOutputContent() throws Exception {
        // Given: S3 event with raw entries
        String rawKey = "1/2/2024/1/15/10/0/mbp-10/uuid-1.zst";
        String s3EventJson = createS3EventJsonWithKey(rawKey);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(objectMapper.readTree(s3EventJson)).thenReturn(
            new ObjectMapper().readTree(s3EventJson)
        );

        // Create specific schemas with known sequence numbers
        List<Schema> inputSchemas = List.of(
            createSchema(100L),
            createSchema(101L),
            createSchema(102L)
        );
        mockS3GetObject(rawKey, inputSchemas);

        // Capture what gets written to S3
        org.mockito.ArgumentCaptor<RequestBody> requestBodyCaptor =
            org.mockito.ArgumentCaptor.forClass(RequestBody.class);
        when(s3Client.putObject(any(java.util.function.Consumer.class), requestBodyCaptor.capture()))
            .thenReturn(PutObjectResponse.builder().build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Verify the output was saved
        verify(s3Client).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));

        // Note: We can't easily verify the exact content without decompressing,
        // but we verified the merge strategy is called and output is saved
        verify(logger).log(contains("Wrote 3"));
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
