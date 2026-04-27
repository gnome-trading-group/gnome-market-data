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
    // 2024-01-15 is a Monday
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
        // Only 1 distinct weekday — below minimumDays=3, so no anomaly even with extreme value
        // 2024-01-12 is a Friday (weekday, matches Monday entry's day type)
        queryResults.add(buildHourlyStats("2024-01-12", ENTRY_HOUR, "tickCount", 29.0, 29000.0, 29000000.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<QualityIssue> issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testAnomalyDetectedAfterMinimumDays() {
        // 3 distinct weekday days (Wed/Thu/Fri), combined mean=1000, stddev=0
        // 2024-01-10=Wed, 2024-01-11=Thu, 2024-01-12=Fri
        queryResults.add(buildHourlyStats("2024-01-10", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-11", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-12", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        // 5 records when mean=1000, stddev=0 — fallback: 5 < 100 → anomalous
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
        // 3 weekday rows older than freshnessDays=9 — freshness check suppresses detection
        // 2023-12-18=Mon, 2023-12-19=Tue, 2023-12-20=Wed (26-28 days before 2024-01-15)
        queryResults.add(buildHourlyStats("2023-12-18", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2023-12-19", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2023-12-20", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        // 5 records — would be anomalous against mean=1000 if freshness passed
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
        // 3 fresh weekday days to trigger anomaly (Wed/Thu/Fri before Monday entry)
        queryResults.add(buildHourlyStats("2024-01-10", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-11", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-12", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));

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
        // SpreadStatistic has lookbackDays=21; rows from 26+ days ago should be excluded
        // 2023-12-16=Mon, 2023-12-17=Tue, 2023-12-18=Wed (26-30 days before 2024-01-15)
        queryResults.add(buildHourlyStats("2023-12-16", ENTRY_HOUR, "spread", 3.0, 30.0, 300.0));
        queryResults.add(buildHourlyStats("2023-12-17", ENTRY_HOUR, "spread", 3.0, 30.0, 300.0));
        queryResults.add(buildHourlyStats("2023-12-18", ENTRY_HOUR, "spread", 3.0, 30.0, 300.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new SpreadStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        // SpreadStatistic lookback=21 days. entryDate=2024-01-15, startDate=2023-12-25.
        // All three rows (Dec 16-18) are outside the 21-day window → filtered out → distinctDays=0 → no anomaly
        List<QualityIssue> issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testUpdateStatisticsFalseNeverCallsUpdateItem() {
        StatisticalQualityRule noWriteRule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()), false, false);
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        noWriteRule.check(entry, List.of(), listing, clock);

        verify(dynamoDbClient, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void testDetectAnomaliesFalseNoIssuesEvenWithAnomaly() {
        // 3 fresh weekday days — would trigger anomaly if detection were enabled
        queryResults.add(buildHourlyStats("2024-01-10", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-11", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-12", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0));

        StatisticalQualityRule noDetectRule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()), false, false);
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        List<QualityIssue> issues = noDetectRule.check(entry, records, listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testWeekdayEntryExcludesWeekendRows() {
        // Monday entry. 3 weekend rows + 2 weekday rows. After day-type filter, only 2 weekday
        // rows remain → distinctDays=2 < minimumDays=3 → no anomaly despite extreme value.
        queryResults.add(buildHourlyStats("2024-01-13", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Sat
        queryResults.add(buildHourlyStats("2024-01-14", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Sun
        queryResults.add(buildHourlyStats("2024-01-06", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Sat
        queryResults.add(buildHourlyStats("2024-01-11", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Thu
        queryResults.add(buildHourlyStats("2024-01-12", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Fri

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        List<QualityIssue> issues = rule.check(entry, records, listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testWeekendEntryUsesWeekendBaseline() {
        // Saturday entry. 3 weekend rows → warmup passes and anomaly is detected using
        // weekend-only baseline (weekday rows are excluded).
        // 2024-01-13=Sat (entry), 2024-01-07=Sun, 2024-01-06=Sat, 2024-01-14=Sun
        queryResults.add(buildHourlyStats("2024-01-06", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Sat
        queryResults.add(buildHourlyStats("2024-01-07", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Sun
        queryResults.add(buildHourlyStats("2024-01-13", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Sat
        // Weekday rows that would inflate baseline if not filtered:
        queryResults.add(buildHourlyStats("2024-01-08", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Mon
        queryResults.add(buildHourlyStats("2024-01-09", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Tue
        queryResults.add(buildHourlyStats("2024-01-10", ENTRY_HOUR, "tickCount", 10.0, 10000.0, 10000000.0)); // Wed

        LocalDateTime saturdayMinute = LocalDateTime.of(2024, 1, 13, 10, 30); // Saturday
        Clock satClock = Clock.fixed(saturdayMinute.atZone(ZoneId.of("UTC")).toInstant(), ZoneId.of("UTC"));
        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, saturdayMinute, MarketDataEntry.EntryType.AGGREGATED);

        // 5 records against weekend mean=1000, stddev=0 → fallback: 5 < 100 → anomalous
        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        List<QualityIssue> issues = rule.check(entry, records, listing, satClock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.TICK_COUNT_ANOMALY, issues.get(0).getRuleType());
    }

    @Test
    void testHourWindowWrapsAtDayBoundary() {
        // MidPriceStatistic uses hourWindow=0; verify a TickCountStatistic at hour=0 queries
        // hours 23, 0, 1. The mock returns the same data for all three queries but SK dedup
        // ensures rows are not counted multiple times.
        LocalDateTime midnightMinute = LocalDateTime.of(2024, 1, 15, 0, 30); // Monday midnight
        Clock midnightClock =
                Clock.fixed(midnightMinute.atZone(ZoneId.of("UTC")).toInstant(), ZoneId.of("UTC"));

        // 3 rows with hour=0 in SK (dedup by SK keeps 1 copy each regardless of 3-hour query)
        queryResults.add(buildHourlyStats("2024-01-10", 0, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-11", 0, "tickCount", 10.0, 10000.0, 10000000.0));
        queryResults.add(buildHourlyStats("2024-01-12", 0, "tickCount", 10.0, 10000.0, 10000000.0));

        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, dynamoDbClient, "test-table", List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, midnightMinute, MarketDataEntry.EntryType.AGGREGATED);

        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        // Should detect anomaly (3 distinct weekday days, mean=1000, value=5 < 100 fallback)
        List<QualityIssue> issues = rule.check(entry, records, listing, midnightClock);

        assertEquals(1, issues.size());
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
