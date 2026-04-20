package group.gnometrading.quality.rules;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.ListingStatistics;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.model.QualityIssueStatus;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.statistics.MidPriceStatistic;
import group.gnometrading.quality.statistics.SpreadStatistic;
import group.gnometrading.quality.statistics.TickCountStatistic;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.Statics;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;

@ExtendWith(MockitoExtension.class)
class StatisticalQualityRuleTest {

    @Mock
    private DynamoDbTable<ListingStatistics> statisticsTable;

    @Mock
    private Listing listing;

    @Mock
    private Exchange exchange;

    @Mock
    private Security security;

    private Clock clock;
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);
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
    }

    @Test
    void testNewListingCreatesFreshStatistics() {
        when(statisticsTable.getItem(any(Key.class))).thenReturn(null);
        StatisticalQualityRule rule = new StatisticalQualityRule(statisticsTable, List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<QualityIssue> issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
        ArgumentCaptor<ListingStatistics> captor = ArgumentCaptor.forClass(ListingStatistics.class);
        verify(statisticsTable).putItem(captor.capture());
        ListingStatistics saved = captor.getValue();
        assertEquals(LISTING_ID, saved.getListingId());
        assertEquals(1, saved.getSampleCount("tickCount"));
    }

    @Test
    void testWelfordMeanUpdatedCorrectly() {
        when(statisticsTable.getItem(any(Key.class))).thenReturn(null);
        StatisticalQualityRule rule = new StatisticalQualityRule(statisticsTable, List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        rule.check(entry, records, listing, clock);

        ArgumentCaptor<ListingStatistics> captor = ArgumentCaptor.forClass(ListingStatistics.class);
        verify(statisticsTable).putItem(captor.capture());
        ListingStatistics saved = captor.getValue();
        assertEquals(1, saved.getSampleCount("tickCount"));
        assertEquals(5.0, saved.getMean("tickCount"), 0.001);
    }

    @Test
    void testExistingStatisticsLoadedAndUpdated() {
        ListingStatistics existing = buildStats("tickCount", 100.0, 5000.0, 10.0);
        when(statisticsTable.getItem(any(Key.class))).thenReturn(existing);
        StatisticalQualityRule rule = new StatisticalQualityRule(statisticsTable, List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 120; i++) records.add(new Mbp10Schema());

        rule.check(entry, records, listing, clock);

        ArgumentCaptor<ListingStatistics> captor = ArgumentCaptor.forClass(ListingStatistics.class);
        verify(statisticsTable).putItem(captor.capture());
        ListingStatistics saved = captor.getValue();
        assertEquals(11, saved.getSampleCount("tickCount"));
        assertTrue(saved.getMean("tickCount") > 100.0);
    }

    @Test
    void testNoAnomalyDuringWarmup() {
        // count=29, below warmup threshold of 30
        ListingStatistics existing = buildStats("tickCount", 1000.0, 0.0, 29.0);
        when(statisticsTable.getItem(any(Key.class))).thenReturn(existing);
        StatisticalQualityRule rule = new StatisticalQualityRule(statisticsTable, List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        // Zero records — would be anomalous if past warmup
        List<QualityIssue> issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testAnomalyDetectedAfterWarmup() {
        ListingStatistics existing = buildStats("tickCount", 1000.0, 100000.0, 30.0);
        when(statisticsTable.getItem(any(Key.class))).thenReturn(existing);
        StatisticalQualityRule rule = new StatisticalQualityRule(statisticsTable, List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        // 5 records when mean is 1000 — well below 10% threshold
        List<Schema> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) records.add(new Mbp10Schema());

        List<QualityIssue> issues = rule.check(entry, records, listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.TICK_COUNT_ANOMALY, issues.get(0).getRuleType());
        assertEquals(QualityIssueStatus.UNREVIEWED, issues.get(0).getStatus());
        assertEquals(LISTING_ID, issues.get(0).getListingId());
    }

    @Test
    void testNaNValueSkippedForIncompatibleSchema() {
        when(statisticsTable.getItem(any(Key.class))).thenReturn(null);
        // SpreadStatistic returns NaN for non-MBP_10
        StatisticalQualityRule rule = new StatisticalQualityRule(statisticsTable, List.of(new SpreadStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_1, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        List<QualityIssue> issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
        ArgumentCaptor<ListingStatistics> captor = ArgumentCaptor.forClass(ListingStatistics.class);
        verify(statisticsTable).putItem(captor.capture());
        ListingStatistics saved = captor.getValue();
        assertTrue(saved.getStatistics() == null || !saved.getStatistics().containsKey("spread"));
    }

    @Test
    void testStatisticsAlwaysWrittenBackAfterCheck() {
        when(statisticsTable.getItem(any(Key.class))).thenReturn(null);
        StatisticalQualityRule rule = new StatisticalQualityRule(statisticsTable, List.of(new TickCountStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        rule.check(entry, List.of(), listing, clock);

        verify(statisticsTable, times(1)).putItem(any(ListingStatistics.class));
    }

    @Test
    void testMultipleStatisticsAllUpdated() {
        when(statisticsTable.getItem(any(Key.class))).thenReturn(null);
        StatisticalQualityRule rule = new StatisticalQualityRule(
                statisticsTable, List.of(new TickCountStatistic(), new SpreadStatistic(), new MidPriceStatistic()));
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.bidPrice0(100);
        s.encoder.askPrice0(102);

        rule.check(entry, List.of(s), listing, clock);

        ArgumentCaptor<ListingStatistics> captor = ArgumentCaptor.forClass(ListingStatistics.class);
        verify(statisticsTable).putItem(captor.capture());
        ListingStatistics saved = captor.getValue();
        assertEquals(1, saved.getSampleCount("tickCount"));
        assertEquals(1, saved.getSampleCount("spread"));
        assertEquals(1, saved.getSampleCount("midPrice"));
        assertEquals(1.0, saved.getMean("tickCount"), 0.001);
        assertEquals(2.0 / Statics.PRICE_SCALING_FACTOR, saved.getMean("spread"), 1e-12);
        assertEquals(101.0 / Statics.PRICE_SCALING_FACTOR, saved.getMean("midPrice"), 1e-12);
    }

    @Test
    void testIssueContainsCorrectMetadata() {
        ListingStatistics existing = buildStats("tickCount", 1000.0, 100000.0, 30.0);
        when(statisticsTable.getItem(any(Key.class))).thenReturn(existing);
        StatisticalQualityRule rule = new StatisticalQualityRule(statisticsTable, List.of(new TickCountStatistic()));
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

    private ListingStatistics buildStats(String metricName, double mean, double m2, double count) {
        ListingStatistics stats = new ListingStatistics();
        stats.setListingId(LISTING_ID);
        Map<String, Map<String, Double>> statsMap = new HashMap<>();
        Map<String, Double> metricMap = new HashMap<>();
        metricMap.put(ListingStatistics.MEAN_KEY, mean);
        metricMap.put(ListingStatistics.M2_KEY, m2);
        metricMap.put(ListingStatistics.COUNT_KEY, count);
        statsMap.put(metricName, metricMap);
        stats.setStatistics(statsMap);
        return stats;
    }
}
