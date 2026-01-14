package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.MarketDataEntry;
import group.gnometrading.SecurityMaster;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.converters.SchemaConversionRegistry;
import group.gnometrading.sm.Listing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive test suite for JobCreatorLambdaHandler.
 * Tests SQS event parsing, job creation logic, schema type filtering, and duplicate handling.
 */
@ExtendWith(MockitoExtension.class)
class JobCreatorLambdaHandlerTest {

    private ObjectMapper objectMapper;

    @Mock
    private SecurityMaster securityMaster;

    @Mock
    private DynamoDbTable<TransformationJob> transformJobsTable;

    @Mock
    private Context context;

    @Mock
    private LambdaLogger logger;

    @Mock
    private Listing listing;

    private Clock clock;
    private JobCreatorLambdaHandler handler;

    private static final LocalDateTime FIXED_TIME = LocalDateTime.of(2024, 1, 15, 10, 30);

    @BeforeEach
    void setUp() {
        // Use a real ObjectMapper for JSON parsing
        objectMapper = new ObjectMapper();

        // Fixed clock for deterministic createdAt timestamps
        clock = Clock.fixed(
                FIXED_TIME.atZone(ZoneId.of("UTC")).toInstant(),
                ZoneId.of("UTC")
        );

        // Initialize handler with mocks
        handler = new JobCreatorLambdaHandler(
                securityMaster,
                objectMapper,
                transformJobsTable,
                clock
        );

        // Setup context to return logger
        lenient().when(context.getLogger()).thenReturn(logger);

        // Setup default listing mock
        lenient().when(listing.listingId()).thenReturn(123);
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

        // Then: Returns null and no jobs created
        assertNull(result);
        verify(transformJobsTable, never()).putItem(any(Consumer.class));
    }

    @Test
    void testS3EventParsing() throws Exception {
        // Given: S3 event for a single merged MBP_10 file
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        // Mock SecurityMaster to return listing
        when(securityMaster.getListing(2, 1)).thenReturn(listing);

        // When: Handling the request (without mocking SchemaConversionRegistry)
        handler.handleRequest(event, context);

        // Then: Should have called getListing (proving the event was parsed)
        verify(securityMaster, atLeastOnce()).getListing(2, 1);
    }

    @Test
    void testHandleRequestWithMalformedJSON() {
        // Given: SQS event with malformed JSON
        String malformedJson = "{invalid json";
        SQSEvent event = createSQSEvent(malformedJson);

        // When: Handling request with malformed JSON
        // S3Utils catches exceptions and returns empty set, so no exception is thrown
        Void result = handler.handleRequest(event, context);

        // Then: Should handle gracefully
        assertNull(result);
        verify(transformJobsTable, never()).putItem(any(Consumer.class));
    }

    // ============================================================================
    // JOB CREATION TESTS
    // ============================================================================

    @Test
    void testCreateJobsForSingleMergedFile() throws Exception {
        // Given: S3 event for a single merged MBP_10 file
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        // Mock SecurityMaster to return listing
        when(securityMaster.getListing(2, 1)).thenReturn(listing);

        // When: Handling the request (using real SchemaConversionRegistry)
        handler.handleRequest(event, context);

        // Then: Should create jobs for each convertible schema type
        // We verify that putItem was called at least once (actual count depends on real converters)
        verify(transformJobsTable, atLeastOnce()).putItem(any(Consumer.class));
        verify(securityMaster, atLeastOnce()).getListing(2, 1);
    }

    @Test
    void testJobCreationWithCorrectFields() throws Exception {
        // Given: S3 event for a merged MBP_10 file
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(securityMaster.getListing(2, 1)).thenReturn(listing);

        // Capture the jobs that get created
        List<TransformationJob> capturedJobs = new ArrayList<>();

        // Mock putItem to capture the jobs
        doAnswer(invocation -> {
            Consumer<software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest.Builder<TransformationJob>> consumer =
                    invocation.getArgument(0);
            software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest.Builder<TransformationJob> builder =
                    mock(software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest.Builder.class);

            // Capture the item
            lenient().when(builder.item(any(TransformationJob.class))).thenAnswer(inv -> {
                capturedJobs.add(inv.getArgument(0));
                return builder;
            });
            lenient().when(builder.conditionExpression(any())).thenReturn(builder);
            lenient().when(builder.build()).thenReturn(mock(software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest.class));

            consumer.accept(builder);
            return null;
        }).when(transformJobsTable).putItem(any(Consumer.class));

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Verify at least one job was created with correct fields
        assertFalse(capturedJobs.isEmpty(), "At least one job should be created");
        TransformationJob job = capturedJobs.get(0);
        assertNotNull(job);
        assertEquals(123, job.getListingId());
        assertEquals(LocalDateTime.of(2024, 1, 15, 10, 30), job.getTimestamp());
        assertEquals(TransformationStatus.PENDING, job.getStatus());
        assertEquals(FIXED_TIME, job.getCreatedAt());
        assertNotNull(job.getJobId());
        assertEquals(123, job.getJobId().listingId());
    }

    @Test
    void testDuplicateJobHandling() throws Exception {
        // Given: S3 event for a merged file
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        when(securityMaster.getListing(2, 1)).thenReturn(listing);

        // Mock putItem to throw ConditionalCheckFailedException (job already exists)
        doThrow(ConditionalCheckFailedException.builder().message("Item already exists").build())
                .when(transformJobsTable).putItem(any(Consumer.class));

        // When: Handling the request
        Void result = handler.handleRequest(event, context);

        // Then: Should handle gracefully and not throw exception
        assertNull(result);
        verify(transformJobsTable, atLeastOnce()).putItem(any(Consumer.class));
    }

    // ============================================================================
    // KEY PARSING TESTS
    // ============================================================================

    @Test
    void testKeyParsingWithDifferentTimestamps() throws Exception {
        // Given: S3 events with different timestamps
        String key1 = "1/2/2024/1/15/10/30/mbp-10.zst";
        String key2 = "3/4/2024/12/31/23/59/mbp-10.zst";

        when(securityMaster.getListing(2, 1)).thenReturn(listing);
        Listing listing2 = mock(Listing.class);
        when(listing2.listingId()).thenReturn(456);
        when(securityMaster.getListing(4, 3)).thenReturn(listing2);

        // Test first key
        SQSEvent event1 = createSQSEvent(createS3EventJsonWithKey(key1));
        handler.handleRequest(event1, context);
        verify(securityMaster, atLeastOnce()).getListing(2, 1);

        // Reset and test second key
        reset(securityMaster);
        when(securityMaster.getListing(4, 3)).thenReturn(listing2);
        SQSEvent event2 = createSQSEvent(createS3EventJsonWithKey(key2));
        handler.handleRequest(event2, context);
        verify(securityMaster, atLeastOnce()).getListing(4, 3);
    }

    // ============================================================================
    // MULTIPLE ENTRIES TESTS
    // ============================================================================

    @Test
    void testMultipleEntriesCreateMultipleJobs() throws Exception {
        // Given: S3 event with multiple merged files
        String key1 = "1/2/2024/1/15/10/30/mbp-10.zst";
        String key2 = "3/4/2024/1/15/11/45/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithMultipleKeys(key1, key2);
        SQSEvent event = createSQSEvent(s3EventJson);

        // Mock SecurityMaster for both entries
        when(securityMaster.getListing(2, 1)).thenReturn(listing);
        Listing listing2 = mock(Listing.class);
        when(listing2.listingId()).thenReturn(456);
        when(securityMaster.getListing(4, 3)).thenReturn(listing2);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should create jobs for both entries
        verify(transformJobsTable, atLeastOnce()).putItem(any(Consumer.class));
        verify(securityMaster, atLeastOnce()).getListing(2, 1);
        verify(securityMaster, atLeastOnce()).getListing(4, 3);
    }

    @Test
    void testMultipleSQSMessagesProcessed() throws Exception {
        // Given: Multiple SQS messages
        String key1 = "1/2/2024/1/15/10/30/mbp-10.zst";
        String key2 = "3/4/2024/1/15/11/45/mbp-10.zst";
        String msg1 = createS3EventJsonWithKey(key1);
        String msg2 = createS3EventJsonWithKey(key2);
        SQSEvent event = createSQSEvent(msg1, msg2);

        when(securityMaster.getListing(2, 1)).thenReturn(listing);
        Listing listing2 = mock(Listing.class);
        when(listing2.listingId()).thenReturn(456);
        when(securityMaster.getListing(4, 3)).thenReturn(listing2);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should process both messages
        verify(transformJobsTable, atLeastOnce()).putItem(any(Consumer.class));
        verify(securityMaster, atLeastOnce()).getListing(2, 1);
        verify(securityMaster, atLeastOnce()).getListing(4, 3);
    }

    // ============================================================================
    // OHLCV_1H EDGE CASE TESTS
    // ============================================================================

    @Test
    void testOHLCV1HOnlyCreatedAtMinute59() throws Exception {
        // Given: Two events - one at minute 59, one at minute 30
        String key59 = "1/2/2024/1/15/10/59/mbp-10.zst";
        String key30 = "1/2/2024/1/15/10/30/mbp-10.zst";

        when(securityMaster.getListing(2, 1)).thenReturn(listing);

        // Capture jobs created for minute 59
        List<TransformationJob> jobs59 = new ArrayList<>();
        captureJobsIntoList(jobs59);

        SQSEvent event59 = createSQSEvent(createS3EventJsonWithKey(key59));
        handler.handleRequest(event59, context);

        // Count OHLCV_1H jobs created at minute 59
        long ohlcv1HCount59 = jobs59.stream()
                .filter(job -> job.getSchemaType() == SchemaType.OHLCV_1H)
                .count();

        // Reset mocks and capture jobs for minute 30
        reset(transformJobsTable, securityMaster);
        when(securityMaster.getListing(2, 1)).thenReturn(listing);
        List<TransformationJob> jobs30 = new ArrayList<>();
        captureJobsIntoList(jobs30);

        SQSEvent event30 = createSQSEvent(createS3EventJsonWithKey(key30));
        handler.handleRequest(event30, context);

        // Count OHLCV_1H jobs created at minute 30
        long ohlcv1HCount30 = jobs30.stream()
                .filter(job -> job.getSchemaType() == SchemaType.OHLCV_1H)
                .count();

        // Then: OHLCV_1H should only be created at minute 59
        assertTrue(ohlcv1HCount59 > 0, "OHLCV_1H jobs should be created at minute 59");
        assertEquals(0, ohlcv1HCount30, "OHLCV_1H jobs should NOT be created at minute 30");
    }

    @ParameterizedTest(name = "Minute {0} should create OHLCV_1H job: {1}")
    @MethodSource("ohlcv1HMinuteTestCases")
    void testOHLCV1HMinuteFiltering(int minute, boolean shouldCreateOHLCV1H) throws Exception {
        // Given: Event at specific minute
        String key = String.format("1/2/2024/1/15/10/%d/mbp-10.zst", minute);
        SQSEvent event = createSQSEvent(createS3EventJsonWithKey(key));

        when(securityMaster.getListing(2, 1)).thenReturn(listing);

        // Capture jobs
        List<TransformationJob> capturedJobs = new ArrayList<>();
        captureJobsIntoList(capturedJobs);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Verify OHLCV_1H job creation based on minute
        long ohlcv1HCount = capturedJobs.stream()
                .filter(job -> job.getSchemaType() == SchemaType.OHLCV_1H)
                .count();

        if (shouldCreateOHLCV1H) {
            assertTrue(ohlcv1HCount > 0, "OHLCV_1H job should be created at minute " + minute);
        } else {
            assertEquals(0, ohlcv1HCount, "OHLCV_1H job should NOT be created at minute " + minute);
        }
    }

    static Stream<Arguments> ohlcv1HMinuteTestCases() {
        return Stream.of(
                Arguments.of(0, false),
                Arguments.of(15, false),
                Arguments.of(30, false),
                Arguments.of(45, false),
                Arguments.of(58, false),
                Arguments.of(59, true)
        );
    }

    @Test
    void testNonOHLCV1HSchemaTypesCreatedRegardlessOfMinute() throws Exception {
        // Given: Event at minute 30 (not 59)
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        SQSEvent event = createSQSEvent(createS3EventJsonWithKey(key));

        when(securityMaster.getListing(2, 1)).thenReturn(listing);

        // Capture jobs
        List<TransformationJob> capturedJobs = new ArrayList<>();
        captureJobsIntoList(capturedJobs);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Non-OHLCV_1H jobs should be created
        long nonOhlcv1HCount = capturedJobs.stream()
                .filter(job -> job.getSchemaType() != SchemaType.OHLCV_1H)
                .count();

        assertTrue(nonOhlcv1HCount > 0, "Non-OHLCV_1H jobs should be created at any minute");
    }

    @Test
    void testOHLCV1HTimestampPreservedInJob() throws Exception {
        // Given: Event at minute 59
        String key = "1/2/2024/1/15/10/59/mbp-10.zst";
        SQSEvent event = createSQSEvent(createS3EventJsonWithKey(key));

        when(securityMaster.getListing(2, 1)).thenReturn(listing);

        // Capture jobs
        List<TransformationJob> capturedJobs = new ArrayList<>();
        captureJobsIntoList(capturedJobs);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: OHLCV_1H job should have correct timestamp
        TransformationJob ohlcv1HJob = capturedJobs.stream()
                .filter(job -> job.getSchemaType() == SchemaType.OHLCV_1H)
                .findFirst()
                .orElse(null);

        assertNotNull(ohlcv1HJob, "OHLCV_1H job should be created");
        assertEquals(LocalDateTime.of(2024, 1, 15, 10, 59), ohlcv1HJob.getTimestamp());
        assertEquals(59, ohlcv1HJob.getTimestamp().getMinute());
    }

    // ============================================================================
    // ERROR HANDLING TESTS
    // ============================================================================

    @Test
    void testListingNotFoundHandling() throws Exception {
        // Given: S3 event for a merged file where listing doesn't exist
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        // Mock SecurityMaster to return null (listing not found)
        when(securityMaster.getListing(2, 1)).thenReturn(null);

        // When: Handling the request
        // Should throw NullPointerException when trying to access listing.listingId()
        assertThrows(RuntimeException.class, () -> {
            handler.handleRequest(event, context);
        });
    }

    @Test
    void testEmptyS3EventRecords() throws Exception {
        // Given: SNS message with empty S3 Records array
        Map<String, Object> s3Event = new java.util.HashMap<>();
        s3Event.put("Records", new ArrayList<>());
        String s3EventJson = objectMapper.writeValueAsString(s3Event);
        String snsMessage = wrapInSnsMessage(s3EventJson);
        SQSEvent event = createSQSEvent(snsMessage);

        // When: Handling the request
        Void result = handler.handleRequest(event, context);

        // Then: Should handle gracefully
        assertNull(result);
        verify(transformJobsTable, never()).putItem(any(Consumer.class));
    }

    @Test
    void testInvalidS3KeyFormat() throws Exception {
        // Given: S3 event with invalid key format
        String invalidKey = "invalid/key/format.zst";
        String s3EventJson = createS3EventJsonWithKey(invalidKey);
        SQSEvent event = createSQSEvent(s3EventJson);

        // When: Handling the request
        // Should throw exception due to invalid key format
        assertThrows(RuntimeException.class, () -> {
            handler.handleRequest(event, context);
        });
    }

    // ============================================================================
    // HELPER METHODS
    // ============================================================================

    /**
     * Helper method to capture jobs into a list during putItem calls.
     */
    private void captureJobsIntoList(List<TransformationJob> capturedJobs) {
        doAnswer(invocation -> {
            Consumer<software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest.Builder<TransformationJob>> consumer =
                    invocation.getArgument(0);
            software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest.Builder<TransformationJob> builder =
                    mock(software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest.Builder.class);

            // Capture the item
            lenient().when(builder.item(any(TransformationJob.class))).thenAnswer(inv -> {
                capturedJobs.add(inv.getArgument(0));
                return builder;
            });
            lenient().when(builder.conditionExpression(any())).thenReturn(builder);
            lenient().when(builder.build()).thenReturn(mock(software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest.class));

            consumer.accept(builder);
            return null;
        }).when(transformJobsTable).putItem(any(Consumer.class));
    }

    /**
     * Creates an SQS event with one or more SNS-wrapped S3 event messages.
     */
    private SQSEvent createSQSEvent(String... snsMessages) {
        SQSEvent event = new SQSEvent();
        List<SQSEvent.SQSMessage> records = new ArrayList<>();

        for (String snsMessage : snsMessages) {
            SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
            message.setBody(snsMessage);
            records.add(message);
        }

        event.setRecords(records);
        return event;
    }

    /**
     * Creates an S3 event JSON wrapped in SNS message with a single S3 object key.
     */
    private String createS3EventJsonWithKey(String key) throws Exception {
        // Build the S3 event structure
        Map<String, Object> s3Event = new java.util.HashMap<>();
        List<Map<String, Object>> records = new ArrayList<>();

        Map<String, Object> record = new java.util.HashMap<>();
        Map<String, Object> s3 = new java.util.HashMap<>();
        Map<String, Object> bucket = new java.util.HashMap<>();
        Map<String, Object> object = new java.util.HashMap<>();

        bucket.put("name", "test-merged-bucket");
        object.put("key", key);
        object.put("size", 1024);

        s3.put("bucket", bucket);
        s3.put("object", object);
        record.put("s3", s3);
        records.add(record);
        s3Event.put("Records", records);

        // Convert to JSON string
        String s3EventJson = objectMapper.writeValueAsString(s3Event);

        // Wrap in SNS message
        return wrapInSnsMessage(s3EventJson);
    }

    /**
     * Creates an S3 event JSON wrapped in SNS message with multiple S3 object keys.
     */
    private String createS3EventJsonWithMultipleKeys(String... keys) throws Exception {
        // Build the S3 event structure
        Map<String, Object> s3Event = new java.util.HashMap<>();
        List<Map<String, Object>> records = new ArrayList<>();

        for (String key : keys) {
            Map<String, Object> record = new java.util.HashMap<>();
            Map<String, Object> s3 = new java.util.HashMap<>();
            Map<String, Object> bucket = new java.util.HashMap<>();
            Map<String, Object> object = new java.util.HashMap<>();

            bucket.put("name", "test-merged-bucket");
            object.put("key", key);
            object.put("size", 1024);

            s3.put("bucket", bucket);
            s3.put("object", object);
            record.put("s3", s3);
            records.add(record);
        }

        s3Event.put("Records", records);

        // Convert to JSON string
        String s3EventJson = objectMapper.writeValueAsString(s3Event);

        // Wrap in SNS message
        return wrapInSnsMessage(s3EventJson);
    }

    /**
     * Wraps an S3 event JSON string in an SNS message format.
     */
    private String wrapInSnsMessage(String s3EventJson) throws Exception {
        // Build SNS message structure
        Map<String, Object> snsMessage = new java.util.HashMap<>();
        snsMessage.put("Type", "Notification");
        snsMessage.put("MessageId", "test-message-id");
        snsMessage.put("TopicArn", "arn:aws:sns:us-east-1:123456789012:test-topic");
        snsMessage.put("Subject", "Amazon S3 Notification");
        snsMessage.put("Message", s3EventJson);  // The S3 event JSON as a string
        snsMessage.put("Timestamp", "2024-01-15T10:30:00.000Z");

        return objectMapper.writeValueAsString(snsMessage);
    }
}
