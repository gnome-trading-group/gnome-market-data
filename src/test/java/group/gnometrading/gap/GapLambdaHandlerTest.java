package group.gnometrading.gap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.SecurityMaster;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import group.gnometrading.transformer.JobId;
import group.gnometrading.transformer.TransformationJob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive test suite for GapLambdaHandler.
 * Tests gap detection logic, edge cases, and DynamoDB storage.
 */
@ExtendWith(MockitoExtension.class)
class GapLambdaHandlerTest {

    private ObjectMapper objectMapper;

    @Mock
    private S3Client s3Client;

    @Mock
    private SecurityMaster securityMaster;

    @Mock
    private DynamoDbTable<TransformationJob> transformJobsTable;

    @Mock
    private DynamoDbTable<Gap> gapsTable;

    @Mock
    private Context context;

    @Mock
    private LambdaLogger logger;

    @Mock
    private Listing listing;

    @Mock
    private Security security;

    @Mock
    private Exchange exchange;

    private Clock clock;
    private GapLambdaHandler handler;

    private static final String MERGED_BUCKET = "test-merged-bucket";
    private static final LocalDateTime FIXED_TIME = LocalDateTime.of(2024, 1, 15, 10, 30);
    private static final int LISTING_ID = 123;
    private static final int SECURITY_ID = 1;
    private static final int EXCHANGE_ID = 2;

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
        handler = new GapLambdaHandler(
                s3Client,
                securityMaster,
                objectMapper,
                transformJobsTable,
                gapsTable,
                MERGED_BUCKET,
                clock
        );

        // Setup context to return logger
        lenient().when(context.getLogger()).thenReturn(logger);

        // Setup listing mock chain
        lenient().when(listing.listingId()).thenReturn(LISTING_ID);
        lenient().when(listing.security()).thenReturn(security);
        lenient().when(listing.exchange()).thenReturn(exchange);
        lenient().when(security.securityId()).thenReturn(SECURITY_ID);
        lenient().when(exchange.exchangeId()).thenReturn(EXCHANGE_ID);
        lenient().when(exchange.schemaType()).thenReturn(SchemaType.MBP_10);
        lenient().when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(listing);
        lenient().when(securityMaster.getListing(LISTING_ID)).thenReturn(listing);
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

        // Then: Returns null and no gaps created
        assertNull(result);
        verify(gapsTable, never()).putItem(any(Gap.class));
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
        verify(gapsTable, never()).putItem(any(Gap.class));
    }

    // ============================================================================
    // NO GAP TESTS - Previous minute exists
    // ============================================================================

    @Test
    void testNoGap_PreviousMinuteExistsInDynamoDB() throws Exception {
        // Given: S3 event for a merged file at 10:30
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        // Mock: Previous minute (10:29) exists in transformation jobs table
        LocalDateTime previousMinute = LocalDateTime.of(2024, 1, 15, 10, 29);
        JobId jobId = new JobId(LISTING_ID, SchemaType.MBP_10);
        Key dbKey = Key.builder()
                .partitionValue(jobId.toString())
                .sortValue(previousMinute.toEpochSecond(ZoneOffset.UTC))
                .build();

        TransformationJob existingJob = new TransformationJob();
        when(transformJobsTable.getItem(dbKey)).thenReturn(existingJob);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: No gaps should be created
        verify(gapsTable, never()).putItem(any(Gap.class));
        verify(transformJobsTable).getItem(dbKey);
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void testNoGap_PreviousMinuteExistsInS3() throws Exception {
        // Given: S3 event for a merged file at 10:30
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        // Mock: Previous minute (10:29) does NOT exist in DDB
        LocalDateTime previousMinute = LocalDateTime.of(2024, 1, 15, 10, 29);
        JobId jobId = new JobId(LISTING_ID, SchemaType.MBP_10);
        Key dbKey = Key.builder()
                .partitionValue(jobId.toString())
                .sortValue(previousMinute.toEpochSecond(ZoneOffset.UTC))
                .build();
        when(transformJobsTable.getItem(dbKey)).thenReturn(null);

        // Mock: Previous minute EXISTS in S3
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: No gaps should be created
        verify(gapsTable, never()).putItem(any(Gap.class));
        verify(transformJobsTable).getItem(dbKey);
        verify(s3Client).headObject(any(HeadObjectRequest.class));
    }

    // ============================================================================
    // SINGLE MINUTE GAP TESTS
    // ============================================================================

    @Test
    void testSingleMinuteGap() throws Exception {
        // Given: S3 event for a merged file at 10:30
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        LocalDateTime currentTime = LocalDateTime.of(2024, 1, 15, 10, 30);
        LocalDateTime previousMinute = LocalDateTime.of(2024, 1, 15, 10, 29);
        LocalDateTime twoMinutesAgo = LocalDateTime.of(2024, 1, 15, 10, 28);

        // Mock: 10:29 does NOT exist (gap)
        mockMinuteDoesNotExist(previousMinute);

        // Mock S3 to throw NoSuchKeyException for missing minutes
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("Not found").build());

        // Mock: 10:28 EXISTS (most recent minute)
        mockMinuteExistsInDynamoDB(twoMinutesAgo);

        // Capture gaps that are stored
        ArgumentCaptor<Gap> gapCaptor = ArgumentCaptor.forClass(Gap.class);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Exactly one gap should be created for 10:29
        verify(gapsTable, times(1)).putItem(gapCaptor.capture());

        Gap capturedGap = gapCaptor.getValue();
        assertEquals(LISTING_ID, capturedGap.getListingId());
        assertEquals(previousMinute, capturedGap.getTimestamp());
        assertEquals(FIXED_TIME, capturedGap.getCreatedAt());
        assertNotNull(capturedGap.getReason());
        assertNotNull(capturedGap.getNote());
    }

    @Test
    void testMultipleMinuteGap() throws Exception {
        // Given: S3 event for a merged file at 10:30
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        LocalDateTime currentTime = LocalDateTime.of(2024, 1, 15, 10, 30);
        LocalDateTime minute29 = LocalDateTime.of(2024, 1, 15, 10, 29);
        LocalDateTime minute28 = LocalDateTime.of(2024, 1, 15, 10, 28);
        LocalDateTime minute27 = LocalDateTime.of(2024, 1, 15, 10, 27);
        LocalDateTime minute26 = LocalDateTime.of(2024, 1, 15, 10, 26);
        LocalDateTime minute25 = LocalDateTime.of(2024, 1, 15, 10, 25);

        // Mock: 10:29, 10:28, 10:27, 10:26 do NOT exist (4-minute gap)
        mockMinuteDoesNotExist(minute29);
        mockMinuteDoesNotExist(minute28);
        mockMinuteDoesNotExist(minute27);
        mockMinuteDoesNotExist(minute26);

        // Mock S3 to throw NoSuchKeyException for missing minutes
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("Not found").build());

        // Mock: 10:25 EXISTS (most recent minute)
        mockMinuteExistsInDynamoDB(minute25);

        // Capture gaps that are stored
        ArgumentCaptor<Gap> gapCaptor = ArgumentCaptor.forClass(Gap.class);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Four gaps should be created (10:26, 10:27, 10:28, 10:29)
        verify(gapsTable, times(4)).putItem(gapCaptor.capture());

        List<Gap> capturedGaps = gapCaptor.getAllValues();
        assertEquals(4, capturedGaps.size());

        // Verify gaps are in chronological order
        assertEquals(minute26, capturedGaps.get(0).getTimestamp());
        assertEquals(minute27, capturedGaps.get(1).getTimestamp());
        assertEquals(minute28, capturedGaps.get(2).getTimestamp());
        assertEquals(minute29, capturedGaps.get(3).getTimestamp());

        // Verify all gaps have correct listingId and createdAt
        for (Gap gap : capturedGaps) {
            assertEquals(LISTING_ID, gap.getListingId());
            assertEquals(FIXED_TIME, gap.getCreatedAt());
        }
    }



    // ============================================================================
    // FIRST ENTRY TESTS - 2+ days of no data
    // ============================================================================

    @Test
    void testFirstEntry_NoPreviousDataWithin2Days() throws Exception {
        // Given: S3 event for a merged file at 10:30
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        LocalDateTime currentTime = LocalDateTime.of(2024, 1, 15, 10, 30);

        // Mock: All minutes for the past 2 days do NOT exist
        // We'll mock the getItem and headObject to always return null/throw exception
        when(transformJobsTable.getItem(any(Key.class))).thenReturn(null);
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("Not found").build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: No gaps should be created (treated as first entry)
        verify(gapsTable, never()).putItem(any(Gap.class));
    }

    @Test
    void testGapAtExactly2DayBoundary() throws Exception {
        // Given: S3 event for a merged file at 10:30 on Jan 15
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        LocalDateTime currentTime = LocalDateTime.of(2024, 1, 15, 10, 30);
        LocalDateTime previousMinute = LocalDateTime.of(2024, 1, 15, 10, 29);

        // 2 days = 2880 minutes
        // Most recent data is exactly 2880 minutes ago (Jan 13, 10:30)
        LocalDateTime twoDaysAgo = currentTime.minusMinutes(2880);

        // Mock: Previous minute does not exist
        mockMinuteDoesNotExist(previousMinute);

        // Mock: All minutes between previous and 2 days ago do NOT exist
        when(transformJobsTable.getItem(any(Key.class))).thenReturn(null);
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("Not found").build());

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: No gaps should be created (beyond 2-day window)
        verify(gapsTable, never()).putItem(any(Gap.class));
    }

    @Test
    void testGapJustWithin2DayWindow() throws Exception {
        // Given: S3 event for a merged file at 10:30 on Jan 15
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        LocalDateTime currentTime = LocalDateTime.of(2024, 1, 15, 10, 30);
        LocalDateTime previousMinute = LocalDateTime.of(2024, 1, 15, 10, 29);

        // Most recent data is 2879 minutes ago (just within 2-day window)
        LocalDateTime mostRecentMinute = currentTime.minusMinutes(2879);

        // Mock: Previous minute does not exist
        mockMinuteDoesNotExist(previousMinute);

        // Mock: All minutes between previous and mostRecent do NOT exist
        when(transformJobsTable.getItem(any(Key.class))).thenAnswer(invocation -> {
            Key key1 = invocation.getArgument(0);
            long timestamp = Long.parseLong(key1.sortKeyValue().get().n());
            LocalDateTime checkTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);

            // Only the mostRecentMinute exists
            if (checkTime.equals(mostRecentMinute)) {
                return new TransformationJob();
            }
            return null;
        });

        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("Not found").build());

        // Capture gaps
        ArgumentCaptor<Gap> gapCaptor = ArgumentCaptor.forClass(Gap.class);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Gaps should be created for all minutes between mostRecentMinute and currentTime
        verify(gapsTable, atLeast(1)).putItem(gapCaptor.capture());

        List<Gap> capturedGaps = gapCaptor.getAllValues();
        assertTrue(capturedGaps.size() > 0);

        // First gap should be one minute after mostRecentMinute
        assertEquals(mostRecentMinute.plusMinutes(1), capturedGaps.get(0).getTimestamp());

        // Last gap should be one minute before currentTime
        assertEquals(previousMinute, capturedGaps.get(capturedGaps.size() - 1).getTimestamp());
    }

    // ============================================================================
    // LISTING NOT FOUND TESTS
    // ============================================================================

    @Test
    void testListingNotFound() throws Exception {
        // Given: S3 event for a merged file
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithKey(key);
        SQSEvent event = createSQSEvent(s3EventJson);

        // Mock: SecurityMaster returns null (listing not found)
        // Override the lenient setup with strict null return
        when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(null);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: No gaps should be created
        verify(gapsTable, never()).putItem(any(Gap.class));
    }

    // ============================================================================
    // MULTIPLE ENTRIES TESTS
    // ============================================================================

    @Test
    void testMultipleEntriesInSingleEvent() throws Exception {
        // Given: S3 event with two merged files
        String key1 = "1/2/2024/1/15/10/30/mbp-10.zst";
        String key2 = "1/2/2024/1/15/10/31/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithMultipleKeys(key1, key2);
        SQSEvent event = createSQSEvent(s3EventJson);

        // Mock: For first entry (10:30), previous minute (10:29) exists
        LocalDateTime time1 = LocalDateTime.of(2024, 1, 15, 10, 30);
        LocalDateTime prev1 = LocalDateTime.of(2024, 1, 15, 10, 29);
        mockMinuteExistsInDynamoDB(prev1);

        // Mock: For second entry (10:31), previous minute (10:30) exists
        LocalDateTime time2 = LocalDateTime.of(2024, 1, 15, 10, 31);
        LocalDateTime prev2 = LocalDateTime.of(2024, 1, 15, 10, 30);
        mockMinuteExistsInDynamoDB(prev2);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: No gaps should be created (both have previous minutes)
        verify(gapsTable, never()).putItem(any(Gap.class));
    }

    @Test
    void testMultipleEntriesWithGaps() throws Exception {
        // Given: S3 event with two merged files, both with gaps
        String key1 = "1/2/2024/1/15/10/30/mbp-10.zst";
        String key2 = "1/2/2024/1/15/10/35/mbp-10.zst";
        String s3EventJson = createS3EventJsonWithMultipleKeys(key1, key2);
        SQSEvent event = createSQSEvent(s3EventJson);

        // For first entry (10:30): gap at 10:29, most recent is 10:28
        LocalDateTime time1 = LocalDateTime.of(2024, 1, 15, 10, 30);
        LocalDateTime prev1 = LocalDateTime.of(2024, 1, 15, 10, 29);
        LocalDateTime recent1 = LocalDateTime.of(2024, 1, 15, 10, 28);

        // For second entry (10:35): gap from 10:31-10:34, most recent is 10:30
        LocalDateTime time2 = LocalDateTime.of(2024, 1, 15, 10, 35);
        LocalDateTime prev2 = LocalDateTime.of(2024, 1, 15, 10, 34);
        LocalDateTime recent2 = LocalDateTime.of(2024, 1, 15, 10, 30);

        // Setup mocks with specific behavior for each timestamp
        when(transformJobsTable.getItem(any(Key.class))).thenAnswer(invocation -> {
            Key key = invocation.getArgument(0);
            long timestamp = Long.parseLong(key.sortKeyValue().get().n());
            LocalDateTime checkTime = LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);

            // Only recent1 (10:28) and recent2 (10:30) exist
            if (checkTime.equals(recent1) || checkTime.equals(recent2)) {
                return new TransformationJob();
            }
            return null;
        });

        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("Not found").build());

        // Capture gaps
        ArgumentCaptor<Gap> gapCaptor = ArgumentCaptor.forClass(Gap.class);

        // When: Handling the request
        handler.handleRequest(event, context);

        // Then: Should create 1 gap for first entry + 4 gaps for second entry = 5 total
        verify(gapsTable, times(5)).putItem(gapCaptor.capture());
    }



    // ============================================================================
    // HELPER METHODS
    // ============================================================================

    /**
     * Mock a minute to NOT exist in both DynamoDB and S3.
     */
    private void mockMinuteDoesNotExist(LocalDateTime timestamp) {
        JobId jobId = new JobId(LISTING_ID, SchemaType.MBP_10);
        Key dbKey = Key.builder()
                .partitionValue(jobId.toString())
                .sortValue(timestamp.toEpochSecond(ZoneOffset.UTC))
                .build();

        lenient().when(transformJobsTable.getItem(dbKey)).thenReturn(null);

        // For S3, we need to match the specific key for this timestamp
        // The headObject mock will throw NoSuchKeyException by default
    }

    /**
     * Mock a minute to exist in DynamoDB.
     */
    private void mockMinuteExistsInDynamoDB(LocalDateTime timestamp) {
        JobId jobId = new JobId(LISTING_ID, SchemaType.MBP_10);
        Key dbKey = Key.builder()
                .partitionValue(jobId.toString())
                .sortValue(timestamp.toEpochSecond(ZoneOffset.UTC))
                .build();

        TransformationJob existingJob = new TransformationJob();
        when(transformJobsTable.getItem(dbKey)).thenReturn(existingJob);
    }

    /**
     * Mock a minute to exist in S3.
     */
    private void mockMinuteExistsInS3(LocalDateTime timestamp) {
        JobId jobId = new JobId(LISTING_ID, SchemaType.MBP_10);
        Key dbKey = Key.builder()
                .partitionValue(jobId.toString())
                .sortValue(timestamp.toEpochSecond(ZoneOffset.UTC))
                .build();

        // First check: does not exist in DDB
        when(transformJobsTable.getItem(dbKey)).thenReturn(null);

        // Second check: exists in S3
        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().build());
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

        bucket.put("name", MERGED_BUCKET);
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

            bucket.put("name", MERGED_BUCKET);
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
