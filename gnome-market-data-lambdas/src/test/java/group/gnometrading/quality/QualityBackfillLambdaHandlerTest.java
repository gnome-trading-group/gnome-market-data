package group.gnometrading.quality;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.SecurityMaster;
import group.gnometrading.quality.model.HourlyListingStatistic;
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
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
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
    private DynamoDbTable<HourlyListingStatistic> hourlyStatisticsTable;

    @Mock
    private DynamoDbClient dynamoDbClient;

    @Mock
    private Context context;

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
    private final List<HourlyListingStatistic> queryResults = new ArrayList<>();

    @BeforeEach
    void setUp() {
        clock = Clock.fixed(FIXED_TIME.atZone(ZoneId.of("UTC")).toInstant(), ZoneId.of("UTC"));

        handler = new QualityBackfillLambdaHandler(
                s3Client,
                securityMaster,
                qualityIssuesTable,
                hourlyStatisticsTable,
                dynamoDbClient,
                "test-table",
                MERGED_BUCKET,
                clock);

        lenient().when(context.getRemainingTimeInMillis()).thenReturn(900_000);
        lenient().when(listing.listingId()).thenReturn(LISTING_ID);
        lenient().when(listing.security()).thenReturn(security);
        lenient().when(listing.exchange()).thenReturn(exchange);
        lenient().when(security.securityId()).thenReturn(SECURITY_ID);
        lenient().when(exchange.exchangeId()).thenReturn(EXCHANGE_ID);
        lenient().when(exchange.schemaType()).thenReturn(SchemaType.MBP_10);
        lenient().when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(listing);

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

        queryResults.clear();
        PageIterable<HourlyListingStatistic> mockPages = mock(PageIterable.class);
        SdkIterable<HourlyListingStatistic> mockItems = mock(SdkIterable.class);
        lenient().when(mockItems.iterator()).thenAnswer(inv -> queryResults.iterator());
        lenient().when(mockPages.items()).thenReturn(mockItems);
        lenient().when(hourlyStatisticsTable.query(any(QueryConditional.class))).thenReturn(mockPages);
    }

    @Test
    void testModeIssuesDoesNotWriteStatistics() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key));
        long windowStart = LocalDateTime.of(2024, 1, 15, 10, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(1);
        s.encoder.timestampEvent(windowStart + 1000);
        mockS3Data(List.of(s));

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "issues", false), context);

        assertEquals(1, result.get("entriesProcessed"));
        assertEquals(0, result.get("issuesFound"));
        verify(dynamoDbClient, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testModeStatisticsWritesStatistics() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key));
        long windowStart = LocalDateTime.of(2024, 1, 15, 10, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(1);
        s.encoder.timestampEvent(windowStart + 1000);
        mockS3Data(List.of(s));

        handler.handleRequest(payload("2024-01-15", "statistics", false), context);

        verify(dynamoDbClient, atLeastOnce()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testModeStatisticsNoIssuesStored() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key));
        // Bad timestamp — would trigger TIMESTAMP_ALIGNMENT if static rules ran
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.timestampEvent(0L);
        mockS3Data(List.of(s));

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "statistics", false), context);

        assertEquals(0, result.get("issuesFound"));
        verify(qualityIssuesTable, never()).putItem(any(QualityIssue.class));
    }

    @Test
    void testResetStatisticsDeletesOnlyMatchingDate() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key));
        long windowStart = LocalDateTime.of(2024, 1, 15, 10, 30).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(1);
        s.encoder.timestampEvent(windowStart + 1000);
        mockS3Data(List.of(s));

        HourlyListingStatistic targetDateRow = new HourlyListingStatistic();
        targetDateRow.setListingId(LISTING_ID);
        targetDateRow.setSk(HourlyListingStatistic.buildSk(10, "2024-01-15", "tickCount"));
        HourlyListingStatistic otherDateRow = new HourlyListingStatistic();
        otherDateRow.setListingId(LISTING_ID);
        otherDateRow.setSk(HourlyListingStatistic.buildSk(10, "2024-01-10", "tickCount"));

        PageIterable<HourlyListingStatistic> pagesWithRows = mockPagesWithRows(List.of(targetDateRow, otherDateRow));
        PageIterable<HourlyListingStatistic> emptyPages = mockPagesWithRows(List.of());
        when(hourlyStatisticsTable.query(any(QueryConditional.class)))
                .thenReturn(pagesWithRows)
                .thenReturn(emptyPages);

        handler.handleRequest(payload("2024-01-15", "statistics", true), context);

        // Only the 2024-01-15 row is deleted; the 2024-01-10 row survives
        verify(hourlyStatisticsTable, times(1)).deleteItem(any(Key.class));
        verify(dynamoDbClient, atLeastOnce()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testResetStatisticsNotCalledForIssuesMode() throws Exception {
        mockS3List("1/2/2024/1/15/", List.of());

        handler.handleRequest(payload("2024-01-15", "issues", true), context);

        verify(hourlyStatisticsTable, never()).deleteItem(any(Key.class));
    }

    @Test
    void testListingNotFoundThrows() {
        when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(null);

        assertThrows(
                IllegalArgumentException.class,
                () -> handler.handleRequest(payload("2024-01-15", "all", false), context));
    }

    @Test
    void testMissingExchangeIdThrows() {
        Map<String, Object> event = new HashMap<>();
        event.put("securityId", SECURITY_ID);
        event.put("date", "2024-01-15");

        assertThrows(IllegalArgumentException.class, () -> handler.handleRequest(event, context));
    }

    @Test
    void testMissingDateThrows() {
        Map<String, Object> event = new HashMap<>();
        event.put("exchangeId", EXCHANGE_ID);
        event.put("securityId", SECURITY_ID);

        assertThrows(IllegalArgumentException.class, () -> handler.handleRequest(event, context));
    }

    @Test
    void testInvalidDateFormatThrows() {
        Map<String, Object> event = new HashMap<>();
        event.put("exchangeId", EXCHANGE_ID);
        event.put("securityId", SECURITY_ID);
        event.put("date", "not-a-date");

        assertThrows(IllegalArgumentException.class, () -> handler.handleRequest(event, context));
    }

    @Test
    void testInvalidModeThrows() {
        Map<String, Object> event = new HashMap<>();
        event.put("exchangeId", EXCHANGE_ID);
        event.put("securityId", SECURITY_ID);
        event.put("date", "2024-01-15");
        event.put("mode", "bogus");

        assertThrows(IllegalArgumentException.class, () -> handler.handleRequest(event, context));
    }

    @Test
    void testNoDataForDateReturnsZero() throws Exception {
        mockS3List("1/2/2024/1/15/", List.of());

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "all", false), context);

        assertEquals(0, result.get("entriesProcessed"));
        assertEquals(0, result.get("issuesFound"));
        assertEquals("2024-01-15", result.get("date"));
        assertEquals("all", result.get("mode"));
    }

    @Test
    void testPerEntryErrorDoesNotAbortRun() throws Exception {
        String key1 = "1/2/2024/1/15/10/30/mbp-10.zst";
        String key2 = "1/2/2024/1/15/10/31/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key1, key2));

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

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "issues", false), context);

        assertEquals(1, result.get("entriesProcessed"));
    }

    @Test
    void testIssueStoredForTimestampViolation() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        mockS3List("1/2/2024/1/15/", List.of(key));
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.timestampEvent(0L);
        mockS3Data(List.of(s));

        Map<String, Object> result = handler.handleRequest(payload("2024-01-15", "issues", false), context);

        assertEquals(1, result.get("issuesFound"));
        ArgumentCaptor<QualityIssue> captor = ArgumentCaptor.forClass(QualityIssue.class);
        verify(qualityIssuesTable, atLeast(1)).putItem(captor.capture());
        assertTrue(
                captor.getAllValues().stream().anyMatch(i -> i.getRuleType() == QualityRuleType.TIMESTAMP_ALIGNMENT));
    }

    private PageIterable<HourlyListingStatistic> mockPagesWithRows(List<HourlyListingStatistic> rows) {
        PageIterable<HourlyListingStatistic> mockPages = mock(PageIterable.class);
        SdkIterable<HourlyListingStatistic> mockItems = mock(SdkIterable.class);
        lenient().when(mockItems.iterator()).thenReturn(rows.iterator());
        lenient().when(mockPages.items()).thenReturn(mockItems);
        return mockPages;
    }

    private Map<String, Object> payload(String date, String mode, boolean resetStatistics) {
        Map<String, Object> event = new HashMap<>();
        event.put("exchangeId", EXCHANGE_ID);
        event.put("securityId", SECURITY_ID);
        event.put("date", date);
        event.put("mode", mode);
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
