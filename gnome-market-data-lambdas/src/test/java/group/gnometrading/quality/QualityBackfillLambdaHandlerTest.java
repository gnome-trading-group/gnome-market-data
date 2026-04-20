package group.gnometrading.quality;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.SecurityMaster;
import group.gnometrading.quality.model.ListingStatistics;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

@ExtendWith(MockitoExtension.class)
class QualityBackfillLambdaHandlerTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private SecurityMaster securityMaster;

    @Mock
    private DynamoDbTable<QualityIssue> qualityIssuesTable;

    @Mock
    private DynamoDbTable<ListingStatistics> listingStatisticsTable;

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
    private QualityBackfillLambdaHandler handler;

    private static final String MERGED_BUCKET = "test-merged-bucket";
    private static final LocalDateTime FIXED_TIME = LocalDateTime.of(2024, 1, 15, 10, 30);
    private static final int LISTING_ID = 123;
    private static final int SECURITY_ID = 1;
    private static final int EXCHANGE_ID = 2;

    private List<byte[]> s3MockedDataList;
    private int s3GetObjectCallCount;

    @BeforeEach
    void setUp() {
        clock = Clock.fixed(FIXED_TIME.atZone(ZoneId.of("UTC")).toInstant(), ZoneId.of("UTC"));

        handler = new QualityBackfillLambdaHandler(
                s3Client, securityMaster, qualityIssuesTable, listingStatisticsTable, MERGED_BUCKET, clock);

        lenient().when(context.getLogger()).thenReturn(logger);
        lenient().when(context.getRemainingTimeInMillis()).thenReturn(900_000);
        lenient().when(listing.listingId()).thenReturn(LISTING_ID);
        lenient().when(listing.security()).thenReturn(security);
        lenient().when(listing.exchange()).thenReturn(exchange);
        lenient().when(security.securityId()).thenReturn(SECURITY_ID);
        lenient().when(exchange.exchangeId()).thenReturn(EXCHANGE_ID);
        lenient().when(exchange.schemaType()).thenReturn(SchemaType.MBP_10);
        lenient().when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(listing);
        lenient().when(listingStatisticsTable.getItem(any(Key.class))).thenReturn(null);

        s3MockedDataList = new ArrayList<>();
        s3GetObjectCallCount = 0;

        lenient()
                .when(s3Client.getObject(any(java.util.function.Consumer.class)))
                .thenAnswer(invocation -> {
                    if (s3GetObjectCallCount >= s3MockedDataList.size()) {
                        throw new RuntimeException("No more mocked S3 data");
                    }
                    byte[] data = s3MockedDataList.get(s3GetObjectCallCount++);
                    InputStream stream = new ByteArrayInputStream(data);
                    return new ResponseInputStream<>(
                            GetObjectResponse.builder().build(), AbortableInputStream.create(stream));
                });
    }

    @Test
    void testStaticRulesOnlyDoesNotWriteStatistics() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key));
        long windowStart = LocalDateTime.of(2024, 1, 15, 10, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(1);
        s.encoder.timestampEvent(windowStart + 1000);
        mockS3Data(List.of(s));

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "2024-01-15", false, false), context);

        assertEquals(1, result.get("entriesProcessed"));
        assertEquals(0, result.get("issuesFound"));
        assertTrue((boolean) result.get("complete"));
        verify(listingStatisticsTable, never()).putItem(any(ListingStatistics.class));
    }

    @Test
    void testWithStatisticalRulesWritesStatistics() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key));
        long windowStart = LocalDateTime.of(2024, 1, 15, 10, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(1);
        s.encoder.timestampEvent(windowStart + 1000);
        mockS3Data(List.of(s));

        handler.handleRequest(payload("2024-01-15", "2024-01-15", true, false), context);

        verify(listingStatisticsTable, atLeastOnce()).putItem(any(ListingStatistics.class));
    }

    @Test
    void testResetStatisticsDeletesBeforeProcessing() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key));
        long windowStart = LocalDateTime.of(2024, 1, 15, 10, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(1);
        s.encoder.timestampEvent(windowStart + 1000);
        mockS3Data(List.of(s));

        handler.handleRequest(payload("2024-01-15", "2024-01-15", true, true), context);

        verify(listingStatisticsTable).deleteItem(any(Key.class));
        verify(listingStatisticsTable, atLeastOnce()).putItem(any(ListingStatistics.class));
    }

    @Test
    void testResetStatisticsNotCalledWhenStatisticalDisabled() throws Exception {
        mockS3List("1/2/2024/1/15/", List.of());

        handler.handleRequest(payload("2024-01-15", "2024-01-15", false, true), context);

        verify(listingStatisticsTable, never()).deleteItem(any(Key.class));
    }

    @Test
    void testListingNotFoundThrows() {
        when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(null);

        assertThrows(
                IllegalArgumentException.class,
                () -> handler.handleRequest(payload("2024-01-15", "2024-01-15", false, false), context));
    }

    @Test
    void testMissingExchangeIdThrows() {
        Map<String, Object> event = new HashMap<>();
        event.put("securityId", SECURITY_ID);
        event.put("startDate", "2024-01-15");
        event.put("endDate", "2024-01-15");

        assertThrows(IllegalArgumentException.class, () -> handler.handleRequest(event, context));
    }

    @Test
    void testMissingStartDateThrows() {
        Map<String, Object> event = new HashMap<>();
        event.put("exchangeId", EXCHANGE_ID);
        event.put("securityId", SECURITY_ID);
        event.put("endDate", "2024-01-15");

        assertThrows(IllegalArgumentException.class, () -> handler.handleRequest(event, context));
    }

    @Test
    void testInvalidDateFormatThrows() {
        Map<String, Object> event = new HashMap<>();
        event.put("exchangeId", EXCHANGE_ID);
        event.put("securityId", SECURITY_ID);
        event.put("startDate", "not-a-date");
        event.put("endDate", "2024-01-15");

        assertThrows(IllegalArgumentException.class, () -> handler.handleRequest(event, context));
    }

    @Test
    void testTimeBudgetExceededReturnsIncomplete() throws Exception {
        mockS3List("1/2/2024/1/15/", List.of());

        // Return low remaining time after first day processed
        lenient()
                .when(context.getRemainingTimeInMillis())
                .thenReturn(900_000) // day 1 check: enough
                .thenReturn(0); // day 2 check: not enough

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "2024-01-16", false, false), context);

        assertFalse((boolean) result.get("complete"));
        assertEquals("2024-01-15", result.get("lastDateProcessed"));
    }

    @Test
    void testNoDataForDateRangeReturnsComplete() throws Exception {
        mockS3List("1/2/2024/1/15/", List.of());

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "2024-01-15", false, false), context);

        assertEquals(0, result.get("entriesProcessed"));
        assertEquals(0, result.get("issuesFound"));
        assertTrue((boolean) result.get("complete"));
        assertEquals("2024-01-15", result.get("lastDateProcessed"));
    }

    @Test
    void testPerEntryErrorDoesNotAbortRun() throws Exception {
        String key1 = "1/2/2024/1/15/10/30/mbp-10.zst";
        String key2 = "1/2/2024/1/15/10/31/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key1, key2));

        // First entry throws, second succeeds
        long windowStart = LocalDateTime.of(2024, 1, 15, 10, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(1);
        s.encoder.timestampEvent(windowStart + 1000);

        when(s3Client.getObject(any(java.util.function.Consumer.class)))
                .thenThrow(new RuntimeException("corrupt file"))
                .thenAnswer(invocation -> {
                    byte[] data = s3MockedDataList.get(0);
                    InputStream stream = new ByteArrayInputStream(data);
                    return new ResponseInputStream<>(
                            GetObjectResponse.builder().build(), AbortableInputStream.create(stream));
                });
        mockS3Data(List.of(s));

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "2024-01-15", false, false), context);

        // Only one entry successfully processed (the second one)
        assertEquals(1, result.get("entriesProcessed"));
        assertTrue((boolean) result.get("complete"));
    }

    @Test
    void testIssueStoredForTimestampViolation() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key));
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.timestampEvent(0L); // far outside window
        mockS3Data(List.of(s));

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "2024-01-15", false, false), context);

        assertEquals(1, result.get("issuesFound"));
        ArgumentCaptor<QualityIssue> captor = ArgumentCaptor.forClass(QualityIssue.class);
        verify(qualityIssuesTable, atLeast(1)).putItem(captor.capture());
        assertTrue(
                captor.getAllValues().stream().anyMatch(i -> i.getRuleType() == QualityRuleType.TIMESTAMP_ALIGNMENT));
    }

    @Test
    void testMultipleDaysProcessedInOrder() throws Exception {
        String key1 = "1/2/2024/1/15/10/30/mbp-10.zst";
        String key2 = "1/2/2024/1/16/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key1));
        mockS3List("1/2/2024/1/16/", List.of(key2));

        long window1 = LocalDateTime.of(2024, 1, 15, 10, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        long window2 = LocalDateTime.of(2024, 1, 16, 10, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.sequence(1);
        s1.encoder.timestampEvent(window1 + 1000);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.sequence(1);
        s2.encoder.timestampEvent(window2 + 1000);
        mockS3Data(List.of(s1));
        mockS3Data(List.of(s2));

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "2024-01-16", false, false), context);

        assertEquals(2, result.get("entriesProcessed"));
        assertTrue((boolean) result.get("complete"));
        assertEquals("2024-01-16", result.get("lastDateProcessed"));
    }

    private Map<String, Object> payload(
            String startDate, String endDate, boolean includeStatistical, boolean resetStatistics) {
        Map<String, Object> event = new HashMap<>();
        event.put("exchangeId", EXCHANGE_ID);
        event.put("securityId", SECURITY_ID);
        event.put("startDate", startDate);
        event.put("endDate", endDate);
        event.put("includeStatistical", includeStatistical);
        event.put("resetStatistics", resetStatistics);
        return event;
    }

    private void mockS3Data(List<? extends Schema> schemas) throws IOException {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        for (Schema schema : schemas) {
            byte[] bytes = new byte[schema.totalMessageSize()];
            schema.buffer.getBytes(0, bytes, 0, schema.totalMessageSize());
            raw.write(bytes);
        }
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (ZstdOutputStream zstd = new ZstdOutputStream(compressed)) {
            zstd.write(raw.toByteArray());
        }
        s3MockedDataList.add(compressed.toByteArray());
    }

    private void mockS3List(String prefix, List<String> keys) {
        List<S3Object> s3Objects =
                keys.stream().map(key -> S3Object.builder().key(key).build()).toList();

        ListObjectsV2Iterable mockIterable = mock(ListObjectsV2Iterable.class);
        when(mockIterable.contents()).thenAnswer(invocation -> {
            SdkIterable<S3Object> mockSdkIterable = mock(SdkIterable.class);
            when(mockSdkIterable.stream()).thenReturn(s3Objects.stream());
            return mockSdkIterable;
        });

        lenient()
                .when(s3Client.listObjectsV2Paginator(
                        argThat((java.util.function.Consumer<ListObjectsV2Request.Builder> consumer) -> {
                            if (consumer == null) return false;
                            ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder();
                            consumer.accept(builder);
                            ListObjectsV2Request request = builder.build();
                            return request.prefix() != null && request.prefix().equals(prefix);
                        })))
                .thenReturn(mockIterable);
    }
}
