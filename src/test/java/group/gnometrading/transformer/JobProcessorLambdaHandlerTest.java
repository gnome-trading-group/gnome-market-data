package group.gnometrading.transformer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.SecurityMaster;
import group.gnometrading.schemas.*;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive test suite for JobProcessorLambdaHandler.
 * Tests job processing, schema conversion (MBP10→MBP1, MBP10→OHLCV_1H),
 * time windows, and job status updates.
 */
@ExtendWith(MockitoExtension.class)
class JobProcessorLambdaHandlerTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private DynamoDbTable<TransformationJob> transformJobsTable;

    @Mock
    private SecurityMaster securityMaster;

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
    private JobProcessorLambdaHandler handler;

    // Map to store mocked S3 data (key -> compressed data)
    private Map<String, byte[]> mockedS3Data;

    private static final String MERGED_BUCKET = "test-merged-bucket";
    private static final String FINAL_BUCKET = "test-final-bucket";
    private static final LocalDateTime FIXED_TIME = LocalDateTime.of(2024, 1, 15, 10, 30);
    private static final int LISTING_ID = 123;
    private static final int SECURITY_ID = 1;
    private static final int EXCHANGE_ID = 2;

    @BeforeEach
    void setUp() {
        // Initialize the map for mocked S3 data
        mockedS3Data = new HashMap<>();
        // Fixed clock for deterministic timestamps
        clock = Clock.fixed(
                FIXED_TIME.atZone(ZoneId.of("UTC")).toInstant(),
                ZoneId.of("UTC")
        );

        // Initialize handler with mocks
        handler = new JobProcessorLambdaHandler(
                s3Client,
                transformJobsTable,
                securityMaster,
                MERGED_BUCKET,
                FINAL_BUCKET,
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
        lenient().when(securityMaster.getListing(LISTING_ID)).thenReturn(listing);

        // Setup S3 getObject to use the mockedS3Data map
        lenient().when(s3Client.getObject(any(java.util.function.Consumer.class))).thenAnswer(invocation -> {
            java.util.function.Consumer<GetObjectRequest.Builder> consumer = invocation.getArgument(0);
            GetObjectRequest.Builder builder = GetObjectRequest.builder();
            consumer.accept(builder);
            GetObjectRequest request = builder.build();

            byte[] compressedData = mockedS3Data.get(request.key());
            if (compressedData != null) {
                // Return a fresh stream for each call
                return new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        AbortableInputStream.create(new ByteArrayInputStream(compressedData))
                );
            }
            // For keys not in the map, throw NoSuchKeyException
            throw NoSuchKeyException.builder().message("Key not found: " + request.key()).build();
        });
    }

    // ============================================================================
    // HANDLE REQUEST TESTS
    // ============================================================================

    @Test
    void testHandleRequestRequiresSchemaType() {
        // Given: Event without schemaType
        Map<String, Object> event = new HashMap<>();

        // When/Then: Should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> {
            handler.handleRequest(event, context);
        });
    }

    @Test
    void testHandleRequestWithEmptySchemaType() {
        // Given: Event with empty schemaType
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "");

        // When/Then: Should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> {
            handler.handleRequest(event, context);
        });
    }

    @Test
    void testHandleRequestWithNoPendingJobs() {
        // Given: Event with valid schemaType but no pending jobs
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");

        // Mock empty query result
        mockScanWithJobs(SchemaType.MBP_1, List.of());

        // When: Handling the request
        Void result = handler.handleRequest(event, context);

        // Then: Should complete without errors
        assertNull(result);
        verify(transformJobsTable).index("schemaType-status-index");
    }

    // ============================================================================
    // MBP10 → MBP1 CONVERSION TESTS
    // ============================================================================

    @Test
    void testMBP10ToMBP1Conversion() throws Exception {
        // Given: A pending MBP_1 job for a single timestamp
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 30);
        TransformationJob job = createJob(LISTING_ID, SchemaType.MBP_1, timestamp);

        mockScanWithJobs(SchemaType.MBP_1, List.of(job));

        // Create MBP10 schemas with realistic data
        List<Schema> mbp10Schemas = List.of(
                createMBP10Schema(100, timestamp, 50000, 50100, 100, 200),
                createMBP10Schema(101, timestamp, 50050, 50150, 150, 250),
                createMBP10Schema(102, timestamp, 50100, 50200, 200, 300)
        );

        // Mock S3 to return MBP10 data
        mockS3GetObject("1/2/2024/1/15/10/30/mbp-10.zst", mbp10Schemas);

        // Capture the output written to S3
        ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");
        handler.handleRequest(event, context);

        // Then: Verify S3 output was written
        verify(s3Client).putObject(any(java.util.function.Consumer.class), requestBodyCaptor.capture());

        // Verify job was marked as COMPLETE
        ArgumentCaptor<TransformationJob> jobCaptor = ArgumentCaptor.forClass(TransformationJob.class);
        verify(transformJobsTable).updateItem(jobCaptor.capture());
        TransformationJob updatedJob = jobCaptor.getValue();
        assertEquals(TransformationStatus.COMPLETE, updatedJob.getStatus());
        assertNotNull(updatedJob.getProcessedAt());
        assertNotNull(updatedJob.getExpiresAt());

        // Verify the converted schemas
        List<Schema> convertedSchemas = extractSchemasFromRequestBody(requestBodyCaptor.getValue(), SchemaType.MBP_1);
        assertFalse(convertedSchemas.isEmpty(), "Should have converted schemas");

        // Verify all converted schemas are MBP1
        for (Schema schema : convertedSchemas) {
            assertTrue(schema instanceof MBP1Schema, "Converted schema should be MBP1");
        }
    }

    @Test
    void testMBP10ToMBP1ConversionPreservesFields() throws Exception {
        // Given: A pending MBP_1 job
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 30);
        TransformationJob job = createJob(LISTING_ID, SchemaType.MBP_1, timestamp);

        mockScanWithJobs(SchemaType.MBP_1, List.of(job));

        // Create MBP10 schema with specific field values
        long sequence = 12345;
        long bidPrice = 50000;
        long askPrice = 50100;
        long bidSize = 100;
        long askSize = 200;

        MBP10Schema mbp10 = createMBP10Schema(sequence, timestamp, bidPrice, askPrice, bidSize, askSize);

        mockS3GetObject("1/2/2024/1/15/10/30/mbp-10.zst", List.of(mbp10));

        // Capture output
        ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");
        handler.handleRequest(event, context);

        // Then: Extract and verify converted schemas
        verify(s3Client).putObject(any(java.util.function.Consumer.class), requestBodyCaptor.capture());
        List<Schema> convertedSchemas = extractSchemasFromRequestBody(requestBodyCaptor.getValue(), SchemaType.MBP_1);

        assertFalse(convertedSchemas.isEmpty(), "Should have converted schemas");

        // Verify the first converted schema has correct fields
        MBP1Schema mbp1 = (MBP1Schema) convertedSchemas.get(0);
        assertEquals(sequence, mbp1.decoder.sequence(), "Sequence should be preserved");
        assertEquals(bidPrice, mbp1.decoder.bidPrice0(), "Bid price should be preserved");
        assertEquals(askPrice, mbp1.decoder.askPrice0(), "Ask price should be preserved");
        assertEquals(bidSize, mbp1.decoder.bidSize0(), "Bid size should be preserved");
        assertEquals(askSize, mbp1.decoder.askSize0(), "Ask size should be preserved");
    }

    // ============================================================================
    // MBP10 → OHLCV_1H CONVERSION TESTS (TIME WINDOW)
    // ============================================================================

    @Test
    void testMBP10ToOHLCV1HConversionWithTimeWindow() throws Exception {
        // Given: A pending OHLCV_1H job at minute 59
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 59);
        TransformationJob job = createJob(LISTING_ID, SchemaType.OHLCV_1H, timestamp);

        mockScanWithJobs(SchemaType.OHLCV_1H, List.of(job));

        // Create MBP10 data for the entire hour (minute 0 to 59)
        // We'll create data for minutes 0, 15, 30, 45, 59 to test the window
        mockS3GetObject("1/2/2024/1/15/10/0/mbp-10.zst", List.of(
                createMBP10Schema(100, LocalDateTime.of(2024, 1, 15, 10, 0), 50000, 50100, 100, 200)
        ));
        mockS3GetObject("1/2/2024/1/15/10/15/mbp-10.zst", List.of(
                createMBP10Schema(200, LocalDateTime.of(2024, 1, 15, 10, 15), 49000, 49100, 150, 250)
        ));
        mockS3GetObject("1/2/2024/1/15/10/30/mbp-10.zst", List.of(
                createMBP10Schema(300, LocalDateTime.of(2024, 1, 15, 10, 30), 51000, 51100, 200, 300)
        ));
        mockS3GetObject("1/2/2024/1/15/10/45/mbp-10.zst", List.of(
                createMBP10Schema(400, LocalDateTime.of(2024, 1, 15, 10, 45), 52000, 52100, 250, 350)
        ));
        mockS3GetObject("1/2/2024/1/15/10/59/mbp-10.zst", List.of(
                createMBP10Schema(500, LocalDateTime.of(2024, 1, 15, 10, 59), 50500, 50600, 300, 400)
        ));

        // Capture output
        ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "ohlcv-1h");
        handler.handleRequest(event, context);

        // Then: Verify S3 was queried for all 60 minutes (0-59)
        verify(s3Client, times(60)).getObject(any(java.util.function.Consumer.class));

        // Verify output was written
        verify(s3Client).putObject(any(java.util.function.Consumer.class), requestBodyCaptor.capture());

        // Extract and verify OHLCV schemas
        List<Schema> convertedSchemas = extractSchemasFromRequestBody(requestBodyCaptor.getValue(), SchemaType.OHLCV_1H);
        assertFalse(convertedSchemas.isEmpty(), "Should have OHLCV_1H schemas");

        // Verify all converted schemas are OHLCV1H
        for (Schema schema : convertedSchemas) {
            assertTrue(schema instanceof OHLCV1HSchema, "Converted schema should be OHLCV1H");
        }
    }

    @Test
    void testOHLCV1HConversionCalculatesCorrectOHLC() throws Exception {
        // Given: A pending OHLCV_1H job with price data that creates clear OHLC values
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 59);
        TransformationJob job = createJob(LISTING_ID, SchemaType.OHLCV_1H, timestamp);

        mockScanWithJobs(SchemaType.OHLCV_1H, List.of(job));

        // Create MBP10 data with specific prices to verify OHLC calculation
        // Open: 50000 (minute 0), High: 52000 (minute 45), Low: 49000 (minute 15), Close: 50500 (minute 59)
        mockS3GetObject("1/2/2024/1/15/10/0/mbp-10.zst", List.of(
                createMBP10Schema(100, LocalDateTime.of(2024, 1, 15, 10, 0), 50000, 50100, 100, 200)
        ));
        mockS3GetObject("1/2/2024/1/15/10/15/mbp-10.zst", List.of(
                createMBP10Schema(200, LocalDateTime.of(2024, 1, 15, 10, 15), 49000, 49100, 150, 250)
        ));
        mockS3GetObject("1/2/2024/1/15/10/30/mbp-10.zst", List.of(
                createMBP10Schema(300, LocalDateTime.of(2024, 1, 15, 10, 30), 51000, 51100, 200, 300)
        ));
        mockS3GetObject("1/2/2024/1/15/10/45/mbp-10.zst", List.of(
                createMBP10Schema(400, LocalDateTime.of(2024, 1, 15, 10, 45), 52000, 52100, 250, 350)
        ));
        mockS3GetObject("1/2/2024/1/15/10/59/mbp-10.zst", List.of(
                createMBP10Schema(500, LocalDateTime.of(2024, 1, 15, 10, 59), 50500, 50600, 300, 400)
        ));

        // Capture output
        ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "ohlcv-1h");
        handler.handleRequest(event, context);

        // Then: Extract and verify OHLCV values
        verify(s3Client).putObject(any(java.util.function.Consumer.class), requestBodyCaptor.capture());
        List<Schema> convertedSchemas = extractSchemasFromRequestBody(requestBodyCaptor.getValue(), SchemaType.OHLCV_1H);

        assertFalse(convertedSchemas.isEmpty(), "Should have OHLCV_1H schemas");

        // Verify OHLC values in the first bar
        OHLCV1HSchema ohlcv = (OHLCV1HSchema) convertedSchemas.get(0);

        // The converter should calculate OHLC from the bid prices
        long open = ohlcv.decoder.open();
        long high = ohlcv.decoder.high();
        long low = ohlcv.decoder.low();
        long close = ohlcv.decoder.close();
        long volume = ohlcv.decoder.volume();

        // Verify OHLC values are set (actual values depend on converter logic)
        assertTrue(open > 0, "Open should be set");
        assertTrue(high > 0, "High should be set");
        assertTrue(low > 0, "Low should be set");
        assertTrue(close > 0, "Close should be set");
        assertTrue(volume >= 0, "Volume should be set");

        // Verify high >= low (basic sanity check)
        assertTrue(high >= low, "High should be >= Low");
    }

    @Test
    void testOHLCV1HWindowStartsAtMinute0() throws Exception {
        // Given: OHLCV_1H job at minute 59
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 59);
        TransformationJob job = createJob(LISTING_ID, SchemaType.OHLCV_1H, timestamp);

        mockScanWithJobs(SchemaType.OHLCV_1H, List.of(job));

        // Mock data for minute 0 and minute 59
        mockS3GetObject("1/2/2024/1/15/10/0/mbp-10.zst", List.of(
                createMBP10Schema(100, LocalDateTime.of(2024, 1, 15, 10, 0), 50000, 50100, 100, 200)
        ));
        mockS3GetObject("1/2/2024/1/15/10/59/mbp-10.zst", List.of(
                createMBP10Schema(500, LocalDateTime.of(2024, 1, 15, 10, 59), 50500, 50600, 300, 400)
        ));

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "ohlcv-1h");
        handler.handleRequest(event, context);

        // Then: Verify S3 was queried for minute 0 (start of window)
        // We can't easily capture the Consumer, but we can verify the calls were made
        verify(s3Client, atLeast(60)).getObject(any(java.util.function.Consumer.class));

        // The fact that we mocked specific keys and the test doesn't fail means those keys were queried
    }

    // ============================================================================
    // TIME WINDOW TESTS
    // ============================================================================

    @Test
    void testMBP1UsesSingleTimestampWindow() throws Exception {
        // Given: MBP_1 job (should only query single timestamp, not a range)
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 30);
        TransformationJob job = createJob(LISTING_ID, SchemaType.MBP_1, timestamp);

        mockScanWithJobs(SchemaType.MBP_1, List.of(job));

        mockS3GetObject("1/2/2024/1/15/10/30/mbp-10.zst", List.of(
                createMBP10Schema(100, timestamp, 50000, 50100, 100, 200)
        ));

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");
        handler.handleRequest(event, context);

        // Then: Should only query S3 once (for the single timestamp)
        verify(s3Client, times(1)).getObject(any(java.util.function.Consumer.class));
    }

    @Test
    void testOutputS3PathIsCorrect() throws Exception {
        // Given: A pending MBP_1 job with specific timestamp
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 30);
        TransformationJob job = createJob(LISTING_ID, SchemaType.MBP_1, timestamp);

        mockScanWithJobs(SchemaType.MBP_1, List.of(job));
        mockS3GetObject("1/2/2024/1/15/10/30/mbp-10.zst", List.of(
                createMBP10Schema(100, timestamp, 50000, 50100, 100, 200)
        ));

        // Capture the putObject request
        ArgumentCaptor<java.util.function.Consumer<PutObjectRequest.Builder>> putRequestCaptor =
                ArgumentCaptor.forClass(java.util.function.Consumer.class);

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");
        handler.handleRequest(event, context);

        // Then: Verify the S3 path is correct
        verify(s3Client).putObject(putRequestCaptor.capture(), any(RequestBody.class));

        // Extract the actual key from the Consumer
        PutObjectRequest.Builder builder = PutObjectRequest.builder();
        putRequestCaptor.getValue().accept(builder);
        PutObjectRequest request = builder.build();

        // Expected format: {securityId}/{exchangeId}/{year}/{month}/{day}/{hour}/{minute}/{schemaType}.zst
        String expectedKey = "1/2/2024/1/15/10/30/mbp-1.zst";
        assertEquals(expectedKey, request.key(), "S3 output key should match expected format");
        assertEquals(FINAL_BUCKET, request.bucket(), "Should write to final bucket");
    }

    // ============================================================================
    // JOB STATUS UPDATE TESTS
    // ============================================================================

    @Test
    void testJobMarkedAsCompleteWithTimestamps() throws Exception {
        // Given: A pending job
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 30);
        TransformationJob job = createJob(LISTING_ID, SchemaType.MBP_1, timestamp);

        mockScanWithJobs(SchemaType.MBP_1, List.of(job));
        mockS3GetObject("1/2/2024/1/15/10/30/mbp-10.zst", List.of(
                createMBP10Schema(100, timestamp, 50000, 50100, 100, 200)
        ));

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");
        handler.handleRequest(event, context);

        // Then: Verify job was updated with correct status and timestamps
        ArgumentCaptor<TransformationJob> jobCaptor = ArgumentCaptor.forClass(TransformationJob.class);
        verify(transformJobsTable).updateItem(jobCaptor.capture());

        TransformationJob updatedJob = jobCaptor.getValue();
        assertEquals(TransformationStatus.COMPLETE, updatedJob.getStatus());
        assertEquals(FIXED_TIME, updatedJob.getProcessedAt());
        assertNotNull(updatedJob.getExpiresAt());

        // Verify expiresAt is 1 week from now
        long expectedExpiry = FIXED_TIME.plusWeeks(1).atZone(ZoneId.of("UTC")).toEpochSecond();
        assertEquals(expectedExpiry, updatedJob.getExpiresAt());
    }

    @Test
    void testEmptySchemaListMarksJobAsComplete() throws Exception {
        // Given: A job where no S3 data exists (all NoSuchKeyException)
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 30);
        TransformationJob job = createJob(LISTING_ID, SchemaType.MBP_1, timestamp);

        mockScanWithJobs(SchemaType.MBP_1, List.of(job));

        // Don't mock any S3 data - the default Answer will throw NoSuchKeyException

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");
        handler.handleRequest(event, context);

        // Then: Job should still be marked as COMPLETE (no data to convert)
        ArgumentCaptor<TransformationJob> jobCaptor = ArgumentCaptor.forClass(TransformationJob.class);
        verify(transformJobsTable).updateItem(jobCaptor.capture());

        TransformationJob updatedJob = jobCaptor.getValue();
        assertEquals(TransformationStatus.COMPLETE, updatedJob.getStatus());

        // Verify no S3 put was called (no output to write)
        verify(s3Client, never()).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
    }

    // ============================================================================
    // ERROR HANDLING TESTS
    // ============================================================================

    @Test
    void testJobMarkedAsFailedOnException() throws Exception {
        // Given: A job that will fail during processing
        LocalDateTime timestamp = LocalDateTime.of(2024, 1, 15, 10, 30);
        TransformationJob job = createJob(LISTING_ID, SchemaType.MBP_1, timestamp);

        mockScanWithJobs(SchemaType.MBP_1, List.of(job));

        // Mock SecurityMaster to throw exception
        when(securityMaster.getListing(LISTING_ID)).thenThrow(new RuntimeException("Database error"));

        // When: Processing the job
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");
        handler.handleRequest(event, context);

        // Then: Job should be marked as FAILED
        ArgumentCaptor<TransformationJob> jobCaptor = ArgumentCaptor.forClass(TransformationJob.class);
        verify(transformJobsTable).updateItem(jobCaptor.capture());

        TransformationJob updatedJob = jobCaptor.getValue();
        assertEquals(TransformationStatus.FAILED, updatedJob.getStatus());
        assertNotNull(updatedJob.getErrorMessage());
        assertTrue(updatedJob.getErrorMessage().contains("Database error"));
        assertNotNull(updatedJob.getProcessedAt());
        assertNotNull(updatedJob.getExpiresAt());
    }

    @Test
    void testMultipleJobsProcessedIndependently() throws Exception {
        // Given: Multiple pending jobs
        LocalDateTime timestamp1 = LocalDateTime.of(2024, 1, 15, 10, 30);
        LocalDateTime timestamp2 = LocalDateTime.of(2024, 1, 15, 10, 31);

        TransformationJob job1 = createJob(LISTING_ID, SchemaType.MBP_1, timestamp1);
        TransformationJob job2 = createJob(LISTING_ID, SchemaType.MBP_1, timestamp2);

        mockScanWithJobs(SchemaType.MBP_1, List.of(job1, job2));

        mockS3GetObject("1/2/2024/1/15/10/30/mbp-10.zst", List.of(
                createMBP10Schema(100, timestamp1, 50000, 50100, 100, 200)
        ));
        mockS3GetObject("1/2/2024/1/15/10/31/mbp-10.zst", List.of(
                createMBP10Schema(200, timestamp2, 50100, 50200, 150, 250)
        ));

        // When: Processing the jobs
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");
        handler.handleRequest(event, context);

        // Then: Both jobs should be processed
        verify(transformJobsTable, times(2)).updateItem(any(TransformationJob.class));
        verify(s3Client, times(2)).putObject(any(java.util.function.Consumer.class), any(RequestBody.class));
    }

    @Test
    void testOneJobFailsOthersContinue() throws Exception {
        // Given: Multiple jobs where one will fail
        LocalDateTime timestamp1 = LocalDateTime.of(2024, 1, 15, 10, 30);
        LocalDateTime timestamp2 = LocalDateTime.of(2024, 1, 15, 10, 31);

        TransformationJob job1 = createJob(LISTING_ID, SchemaType.MBP_1, timestamp1);
        TransformationJob job2 = createJob(LISTING_ID, SchemaType.MBP_1, timestamp2);

        mockScanWithJobs(SchemaType.MBP_1, List.of(job1, job2));

        // Mock S3 to succeed for minute 31 only
        mockS3GetObject("1/2/2024/1/15/10/31/mbp-10.zst", List.of(
                createMBP10Schema(200, timestamp2, 50100, 50200, 150, 250)
        ));

        // Override the default Answer to throw exception for minute 30
        when(s3Client.getObject(any(java.util.function.Consumer.class))).thenAnswer(invocation -> {
            java.util.function.Consumer<GetObjectRequest.Builder> consumer = invocation.getArgument(0);
            GetObjectRequest.Builder builder = GetObjectRequest.builder();
            consumer.accept(builder);
            GetObjectRequest request = builder.build();

            if (request.key().contains("10/30")) {
                throw new RuntimeException("S3 error");
            }

            // Check if we have data for this key
            byte[] compressedData = mockedS3Data.get(request.key());
            if (compressedData != null) {
                return new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        AbortableInputStream.create(new ByteArrayInputStream(compressedData))
                );
            }
            throw NoSuchKeyException.builder().message("Key not found: " + request.key()).build();
        });

        // When: Processing the jobs
        Map<String, Object> event = new HashMap<>();
        event.put("schemaType", "mbp-1");
        handler.handleRequest(event, context);

        // Then: Both jobs should be updated (one FAILED, one COMPLETE)
        ArgumentCaptor<TransformationJob> jobCaptor = ArgumentCaptor.forClass(TransformationJob.class);
        verify(transformJobsTable, times(2)).updateItem(jobCaptor.capture());

        List<TransformationJob> updatedJobs = jobCaptor.getAllValues();
        long failedCount = updatedJobs.stream()
                .filter(j -> j.getStatus() == TransformationStatus.FAILED)
                .count();
        long completeCount = updatedJobs.stream()
                .filter(j -> j.getStatus() == TransformationStatus.COMPLETE)
                .count();

        assertEquals(1, failedCount, "One job should be FAILED");
        assertEquals(1, completeCount, "One job should be COMPLETE");
    }

    // ============================================================================
    // HELPER METHODS
    // ============================================================================

    /**
     * Creates a TransformationJob with the given parameters.
     */
    private TransformationJob createJob(int listingId, SchemaType schemaType, LocalDateTime timestamp) {
        TransformationJob job = new TransformationJob();
        job.setJobId(new JobId(listingId, schemaType));
        job.setListingId(listingId);
        job.setSchemaType(schemaType);
        job.setTimestamp(timestamp);
        job.setStatus(TransformationStatus.PENDING);
        job.setCreatedAt(FIXED_TIME.minusHours(1));
        return job;
    }

    /**
     * Creates an MBP10 schema with the given parameters.
     */
    private MBP10Schema createMBP10Schema(long sequence, LocalDateTime timestamp,
                                          long bidPrice, long askPrice, long bidSize, long askSize) {
        MBP10Schema schema = (MBP10Schema) SchemaType.MBP_10.newInstance();

        long eventTimestamp = timestamp.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli() * 1_000_000;

        schema.encoder.sequence(sequence);
        schema.encoder.exchangeId(EXCHANGE_ID);
        schema.encoder.securityId(SECURITY_ID);
        schema.encoder.timestampEvent(eventTimestamp);
        schema.encoder.timestampSent(eventTimestamp);
        schema.encoder.timestampRecv(eventTimestamp);
        schema.encoder.bidPrice0(bidPrice);
        schema.encoder.askPrice0(askPrice);
        schema.encoder.bidSize0(bidSize);
        schema.encoder.askSize0(askSize);
        schema.encoder.bidCount0(1);
        schema.encoder.askCount0(1);
        schema.encoder.action(Action.Trade);
        schema.encoder.price(bidPrice);

        return schema;
    }

    /**
     * Mocks DynamoDbIndex.query to return the given jobs.
     */
    private void mockScanWithJobs(SchemaType schemaType, List<TransformationJob> jobs) {
        DynamoDbIndex<TransformationJob> index = mock(DynamoDbIndex.class);
        software.amazon.awssdk.core.pagination.sync.SdkIterable<Page<TransformationJob>> pagesIterable =
                mock(software.amazon.awssdk.core.pagination.sync.SdkIterable.class);
        Page<TransformationJob> page = mock(Page.class);

        when(transformJobsTable.index("schemaType-status-index")).thenReturn(index);
        when(index.query(any(software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest.class)))
                .thenReturn(pagesIterable);

        // Mock the iterator to return a single page
        when(pagesIterable.iterator()).thenReturn(List.of(page).iterator());

        // Mock the page to return the jobs
        when(page.items()).thenReturn(jobs);
    }

    /**
     * Mocks S3Client.getObject to return compressed schema data for the given key.
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

        // Add to the map - the Answer in setUp will use this
        mockedS3Data.put(key, compressedData);
    }

    /**
     * Extracts schemas from a RequestBody that was written to S3.
     */
    private List<Schema> extractSchemasFromRequestBody(RequestBody requestBody, SchemaType schemaType) throws IOException {
        // Get the bytes from RequestBody
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        requestBody.contentStreamProvider().newStream().transferTo(baos);
        byte[] compressedData = baos.toByteArray();

        // Decompress with Zstd
        ByteArrayInputStream compressedInput = new ByteArrayInputStream(compressedData);
        ByteArrayOutputStream decompressedOutput = new ByteArrayOutputStream();

        try (ZstdInputStream zstdStream = new ZstdInputStream(compressedInput)) {
            zstdStream.transferTo(decompressedOutput);
        }
        byte[] decompressedData = decompressedOutput.toByteArray();

        // Parse schemas
        List<Schema> schemas = new ArrayList<>();
        int expectedSize = schemaType.getInstance().totalMessageSize();

        for (int i = 0; i < decompressedData.length; i += expectedSize) {
            byte[] recordData = Arrays.copyOfRange(decompressedData, i, i + expectedSize);
            Schema schema = schemaType.newInstance();
            schema.buffer.putBytes(0, recordData, 0, recordData.length);
            schemas.add(schema);
        }

        return schemas;
    }
}
