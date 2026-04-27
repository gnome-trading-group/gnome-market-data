package group.gnometrading.quality;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.SecurityMaster;
import group.gnometrading.quality.model.HourlyListingStatistic;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.model.QualityIssueStatus;
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
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@ExtendWith(MockitoExtension.class)
class QualityCheckLambdaHandlerTest {

    private ObjectMapper objectMapper;

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
    private QualityCheckLambdaHandler handler;

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
        objectMapper = new ObjectMapper();
        clock = Clock.fixed(FIXED_TIME.atZone(ZoneId.of("UTC")).toInstant(), ZoneId.of("UTC"));

        handler = new QualityCheckLambdaHandler(
                objectMapper,
                s3Client,
                securityMaster,
                qualityIssuesTable,
                MERGED_BUCKET,
                clock,
                hourlyStatisticsTable,
                dynamoDbClient,
                "test-table");

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
    void testEmptyEventReturnsNull() {
        SQSEvent event = new SQSEvent();
        event.setRecords(new ArrayList<>());

        Void result = handler.handleRequest(event, context);

        assertNull(result);
        verify(qualityIssuesTable, never()).putItem(any(QualityIssue.class));
    }

    @Test
    void testMalformedJsonHandledGracefully() {
        SQSEvent event = createSQSEvent("{invalid json");

        Void result = handler.handleRequest(event, context);

        assertNull(result);
        verify(qualityIssuesTable, never()).putItem(any(QualityIssue.class));
    }

    @Test
    void testListingNotFoundSkipsEntry() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        SQSEvent event = createSQSEvent(wrapInSnsMessage(createS3EventJson(key)));
        when(securityMaster.getListing(EXCHANGE_ID, SECURITY_ID)).thenReturn(null);

        handler.handleRequest(event, context);

        verify(qualityIssuesTable, never()).putItem(any(QualityIssue.class));
        verify(s3Client, never()).getObject(any(java.util.function.Consumer.class));
    }

    @Test
    void testCleanDataProducesNoIssues() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        SQSEvent event = createSQSEvent(wrapInSnsMessage(createS3EventJson(key)));

        long windowStartNanos = FIXED_TIME.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.sequence(1);
        s1.encoder.timestampEvent(windowStartNanos + 1000);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.sequence(2);
        s2.encoder.timestampEvent(windowStartNanos + 2000);
        mockS3Data(List.of(s1, s2));

        handler.handleRequest(event, context);

        verify(qualityIssuesTable, never()).putItem(any(QualityIssue.class));
    }

    @Test
    void testTimestampAlignmentViolationCreatesIssue() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        SQSEvent event = createSQSEvent(wrapInSnsMessage(createS3EventJson(key)));

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.timestampEvent(0L);
        mockS3Data(List.of(s));

        handler.handleRequest(event, context);

        ArgumentCaptor<QualityIssue> captor = ArgumentCaptor.forClass(QualityIssue.class);
        verify(qualityIssuesTable, atLeast(1)).putItem(captor.capture());
        assertTrue(
                captor.getAllValues().stream().anyMatch(i -> i.getRuleType() == QualityRuleType.TIMESTAMP_ALIGNMENT));
    }

    @Test
    void testSequenceMonotonicityViolationCreatesIssue() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        SQSEvent event = createSQSEvent(wrapInSnsMessage(createS3EventJson(key)));

        long windowStartNanos = FIXED_TIME.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.sequence(5);
        s1.encoder.timestampEvent(windowStartNanos + 1000);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.sequence(3);
        s2.encoder.timestampEvent(windowStartNanos + 2000);
        mockS3Data(List.of(s1, s2));

        handler.handleRequest(event, context);

        ArgumentCaptor<QualityIssue> captor = ArgumentCaptor.forClass(QualityIssue.class);
        verify(qualityIssuesTable, atLeast(1)).putItem(captor.capture());
        assertTrue(
                captor.getAllValues().stream().anyMatch(i -> i.getRuleType() == QualityRuleType.SEQUENCE_MONOTONICITY));
    }

    @Test
    void testBadDataFlagsViolationCreatesIssue() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        SQSEvent event = createSQSEvent(wrapInSnsMessage(createS3EventJson(key)));

        long windowStartNanos = FIXED_TIME.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(1);
        s.encoder.timestampEvent(windowStartNanos + 1000);
        s.encoder.flags().maybeBadBook(true);
        mockS3Data(List.of(s));

        handler.handleRequest(event, context);

        ArgumentCaptor<QualityIssue> captor = ArgumentCaptor.forClass(QualityIssue.class);
        verify(qualityIssuesTable, atLeast(1)).putItem(captor.capture());
        assertTrue(captor.getAllValues().stream().anyMatch(i -> i.getRuleType() == QualityRuleType.BAD_DATA_FLAGS));
    }

    @Test
    void testColdStartProducesNoStatisticalIssues() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        SQSEvent event = createSQSEvent(wrapInSnsMessage(createS3EventJson(key)));

        long windowStartNanos = FIXED_TIME.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(1);
        s.encoder.timestampEvent(windowStartNanos + 1000);
        mockS3Data(List.of(s));

        handler.handleRequest(event, context);

        verify(qualityIssuesTable, never())
                .putItem(argThat((QualityIssue i) -> i.getRuleType() == QualityRuleType.TICK_COUNT_ANOMALY
                        || i.getRuleType() == QualityRuleType.SPREAD_ANOMALY
                        || i.getRuleType() == QualityRuleType.MID_PRICE_ANOMALY));

        verify(dynamoDbClient, atLeastOnce()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testStatisticalAnomalyCreatesIssue() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        SQSEvent event = createSQSEvent(wrapInSnsMessage(createS3EventJson(key)));

        for (String date : List.of("2024-01-10", "2024-01-11", "2024-01-12")) {
            HourlyListingStatistic baseline = new HourlyListingStatistic();
            baseline.setListingId(LISTING_ID);
            baseline.setSk(HourlyListingStatistic.buildSk(10, date, "tickCount"));
            baseline.setCount(10.0);
            baseline.setSum(10000.0);
            baseline.setSumOfSquares(10000000.0);
            queryResults.add(baseline);
        }

        long windowStartNanos = FIXED_TIME.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.sequence(1);
        s1.encoder.timestampEvent(windowStartNanos + 1000);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.sequence(2);
        s2.encoder.timestampEvent(windowStartNanos + 2000);
        mockS3Data(List.of(s1, s2));

        handler.handleRequest(event, context);

        ArgumentCaptor<QualityIssue> captor = ArgumentCaptor.forClass(QualityIssue.class);
        verify(qualityIssuesTable, atLeast(1)).putItem(captor.capture());
        assertTrue(captor.getAllValues().stream().anyMatch(i -> i.getRuleType() == QualityRuleType.TICK_COUNT_ANOMALY));
    }

    @Test
    void testMultipleEntriesInEventProcessedIndependently() throws Exception {
        String key1 = "1/2/2024/1/15/10/30/mbp-10.zst";
        String key2 = "1/2/2024/1/15/10/31/mbp-10.zst";
        SQSEvent event = createSQSEvent(wrapInSnsMessage(createS3EventJsonWithKeys(key1, key2)));

        long window1StartNanos = FIXED_TIME.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;
        long window2StartNanos = FIXED_TIME.plusMinutes(1).toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.sequence(1);
        s1.encoder.timestampEvent(window1StartNanos + 1000);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.sequence(1);
        s2.encoder.timestampEvent(window2StartNanos + 1000);

        mockS3Data(List.of(s1));
        mockS3Data(List.of(s2));

        handler.handleRequest(event, context);

        // two entries × N statistics each emit updateItem calls
        verify(dynamoDbClient, atLeast(2)).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testIssueHasCorrectListingAndTimestamp() throws Exception {
        String key = "1/2/2024/1/15/10/30/mbp-10.zst";
        SQSEvent event = createSQSEvent(wrapInSnsMessage(createS3EventJson(key)));

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.timestampEvent(0L);
        mockS3Data(List.of(s));

        handler.handleRequest(event, context);

        ArgumentCaptor<QualityIssue> captor = ArgumentCaptor.forClass(QualityIssue.class);
        verify(qualityIssuesTable, atLeast(1)).putItem(captor.capture());
        QualityIssue issue = captor.getAllValues().stream()
                .filter(i -> i.getRuleType() == QualityRuleType.TIMESTAMP_ALIGNMENT)
                .findFirst()
                .orElseThrow();
        assertEquals(LISTING_ID, issue.getListingId());
        assertEquals(FIXED_TIME, issue.getTimestamp());
        assertEquals(QualityIssueStatus.UNREVIEWED, issue.getStatus());
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

    private SQSEvent createSQSEvent(String body) {
        SQSEvent event = new SQSEvent();
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setBody(body);
        event.setRecords(List.of(message));
        return event;
    }

    private String createS3EventJson(String key) {
        return String.format(
                "{\"Records\":[{\"s3\":{\"bucket\":{\"name\":\"%s\"},\"object\":{\"key\":\"%s\",\"size\":1024}}}]}",
                MERGED_BUCKET, key);
    }

    private String createS3EventJsonWithKeys(String... keys) {
        StringBuilder records = new StringBuilder();
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) records.append(",");
            records.append(String.format(
                    "{\"s3\":{\"bucket\":{\"name\":\"%s\"},\"object\":{\"key\":\"%s\",\"size\":1024}}}",
                    MERGED_BUCKET, keys[i]));
        }
        return String.format("{\"Records\":[%s]}", records);
    }

    private String wrapInSnsMessage(String s3EventJson) {
        String escaped = s3EventJson.replace("\\", "\\\\").replace("\"", "\\\"");
        return String.format(
                "{\"Type\":\"Notification\",\"MessageId\":\"test\",\"TopicArn\":\"arn:test\",\"Message\":\"%s\"}",
                escaped);
    }
}
