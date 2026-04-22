package group.gnometrading.quality.rules;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.HourlyListingStatistic;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.model.QualityIssueStatus;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.rules.statistics.MidPriceStatistic;
import group.gnometrading.quality.rules.statistics.SpreadStatistic;
import group.gnometrading.quality.rules.statistics.TickCountStatistic;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@ExtendWith(MockitoExtension.class)
class StatisticalQualityRuleTest {

    @Mock
    private DynamoDbTable<HourlyListingStatistic> statisticsTable;

    @Mock
    private DynamoDbClient dynamoDbClient;

    @Mock
    private Listing listing;

    @Mock
    private Exchange exchange;

    @Mock
    private Security security;

    private Clock clock;
    private final List<HourlyListingStatistic> queryResults = new ArrayList<>();
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);
    private static final int ENTRY_HOUR = 10;
    private static final int LISTING_ID = 42;

    @BeforeEach
    void setUp() {
        clock = Clock.fixed(MINUTE.atZone(ZoneId.of("UTC")).toInstant(), ZoneId.of("UTC"));
        lenient().when(listing.listingId()).thenReturn(LISTING_ID);
        lenient().when(listing.exchange()).thenReturn(exchange);
        lenient().when(listing.security()).thenReturn(security);
        lenient().when(exchange.exchangeId()).thenReturn(1);
        lenient().when(exchange.schemaType()).thenReturn(SchemaType.MBP_10);
        lenient().when(security.securityId()).thenReturn(2);
        queryResults.clear();
        PageIterable<HourlyListingStatistic> mockPages = mock(PageIterable.class);
        SdkIterable<HourlyListingStatistic> mockItems = mock(SdkIterable.class);
        lenient().when(mockItems.iterator()).thenAnswer(inv -> queryResults.iterator());
        lenient().when(mockPages.items()).thenReturn(mockItems);
        lenient().when(statisticsTable.query(any(QueryConditional.class))).thenReturn(mockPages);
    }

    @Test
    void testNewListingEmitsUpdateItem() {
        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<QualityIssue> issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
        verify(dynamoDbClient, times(1)).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testUpdateItemHasCorrectValues() {
        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        rule.check(entry, records, listing, clock);

        ArgumentCaptor<UpdateItemRequest> captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(dynamoDbClient).updateItem(captor.capture());
        UpdateItemRequest req = captor.getValue();
        assertEquals("5.0", req.expressionAttributeValues().get(":val").n());
        assertEquals("25.0", req.expressionAttributeValues().get(":sq").n());
    }

    @Test
    void testInsufficientDaysNoAnomaly() {
        // Only 1 distinct day — below minimumDays=3, so no anomaly even with extreme value
        queryResults.add(buildHourlyStats("2024-01-14", ENTRY_HOUR, "tickCount", 29.0, 29000.0, 29000000.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<QualityIssue> issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testAnomalyDetectedAfterMinimumDays() {
        // 3 distinct recent days, combined mean=1000
        queryResults.add(buildHourlyStats("2024-01-13", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-14", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-15", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        // 5 records when mean=1000 — well below the 10% threshold (100)
        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        List<QualityIssue> issues = rule.check(entry, records, listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.TICK_COUNT_ANOMALY, issues.get(0).getRuleType());
        assertEquals(QualityIssueStatus.UNREVIEWED, issues.get(0).getStatus());
        assertEquals(LISTING_ID, issues.get(0).getListingId());
    }

    @Test
    void testNoAnomalyAfterGap() {
        // 3 distinct days but all from 10+ days ago — freshness check blocks anomaly detection
        queryResults.add(buildHourlyStats("2024-01-04", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-05", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-06", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        // 5 records — would be anomalous against mean=1000 if warmup passed
        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        List<QualityIssue> issues = rule.check(entry, records, listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testNaNValueSkippedForIncompatibleSchema() {
        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new SpreadStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_1, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        rule.check(entry, List.of(), listing, clock);

        verify(dynamoDbClient, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testUpdateItemAlwaysCalledAfterCheck() {
        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        rule.check(entry, List.of(), listing, clock);

        verify(dynamoDbClient, times(1)).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testMultipleStatisticsEachEmitUpdateItem() {
        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable,
                dynamoDbClient,
                "test-table",
                List.of(new TickCountStatistic(), new SpreadStatistic(), new MidPriceStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.bidPrice0(100);
        s.encoder.askPrice0(102);

        rule.check(entry, List.of(s), listing, clock);

        // tickCount, spread, midPrice — all non-NaN for MBP_10 with data
        verify(dynamoDbClient, times(3)).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testIssueContainsCorrectMetadata() {
        // Need 3 fresh distinct days to trigger anomaly
        queryResults.add(buildHourlyStats("2024-01-13", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-14", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-15", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<QualityIssue> issues = rule.check(entry, List.of(), listing, clock);

        assertEquals(1, issues.size());
        QualityIssue issue = issues.get(0);
        assertEquals(MINUTE, issue.getTimestamp());
        assertEquals(MINUTE, issue.getCreatedAt());
        assertNotNull(issue.getIssueId());
        assertNotNull(issue.getDetails());
        assertNotNull(issue.getS3Key());
    }

    @Test
    void testPerStatisticLookbackFiltering() {
        // SpreadStatistic has lookbackDays=7; a row from 10 days ago should be excluded
        queryResults.add(buildHourlyStats("2024-01-05", ENTRY_HOUR, "spread", 3.0, 30.0, 300.0));
        queryResults.add(buildHourlyStats("2024-01-06", ENTRY_HOUR, "spread", 3.0, 30.0, 300.0));
        queryResults.add(buildHourlyStats("2024-01-07", ENTRY_HOUR, "spread", 3.0, 30.0, 300.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new SpreadStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        // SpreadStatistic lookback=7 days. entryDate=2024-01-15, startDate=2024-01-08.
        // All three rows (Jan 5-7) are outside the 7-day window → filtered out → distinctDays=0 → no anomaly
        List<QualityIssue> issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testUpdateStatisticsFalseNeverCallsUpdateItem() {
        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()), true, false);
        // Flip: updateStatistics=false
        StatisticalQualityRule noWriteRule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()), false, false);
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        noWriteRule.check(entry, List.of(), listing, clock);

        verify(dynamoDbClient, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testDetectAnomaliesFalseNoIssuesEvenWithAnomaly() {
        queryResults.add(buildHourlyStats("2024-01-13", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-14", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-15", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));

        StatisticalQualityRule noDetectRule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()), false, false);
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        // 5 records when mean=1000 — anomalous, but detectAnomalies=false so no issues
        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        List<QualityIssue> issues = noDetectRule.check(entry, records, listing, clock);

        assertTrue(issues.isEmpty());
    }

    private HourlyListingStatistic buildHourlyStats(
            String date, int hour, String metric, double count, double sum, double sumOfSquares) {
        HourlyListingStatistic stat = new HourlyListingStatistic();
        stat.setListingId(LISTING_ID);
        stat.setSk(HourlyListingStatistic.buildSk(hour, date, metric));
        stat.setCount(count);
        stat.setSum(sum);
        stat.setSumOfSquares(sumOfSquares);
        return stat;
    }
}
