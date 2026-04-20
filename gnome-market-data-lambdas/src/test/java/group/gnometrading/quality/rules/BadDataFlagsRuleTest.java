package group.gnometrading.quality.rules;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BadDataFlagsRuleTest {

    @Mock
    private Listing listing;

    @Mock
    private Exchange exchange;

    @Mock
    private Security security;

    private final BadDataFlagsRule rule = new BadDataFlagsRule();
    private Clock clock;
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);

    @BeforeEach
    void setUp() {
        clock = Clock.fixed(MINUTE.atZone(ZoneId.of("UTC")).toInstant(), ZoneId.of("UTC"));
        lenient().when(listing.listingId()).thenReturn(123);
        lenient().when(listing.exchange()).thenReturn(exchange);
        lenient().when(listing.security()).thenReturn(security);
        lenient().when(exchange.exchangeId()).thenReturn(1);
        lenient().when(exchange.schemaType()).thenReturn(SchemaType.MBP_10);
        lenient().when(security.securityId()).thenReturn(2);
    }

    @Test
    void testNoIssuesWhenNoFlagsSet() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.flags().maybeBadBook(false);
        s.encoder.flags().badTimestampRecv(false);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testIssueForMaybeBadBookFlag() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.flags().maybeBadBook(true);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.BAD_DATA_FLAGS, issues.get(0).getRuleType());
        assertTrue(issues.get(0).getDetails().contains("1 record(s) with maybeBadBook"));
    }

    @Test
    void testIssueForBadTimestampRecvFlag() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.flags().badTimestampRecv(true);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertEquals(1, issues.size());
        assertTrue(issues.get(0).getDetails().contains("1 record(s) with badTimestampRecv"));
    }

    @Test
    void testIssueDetailsCountsBothFlagTypes() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.flags().maybeBadBook(true);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.flags().badTimestampRecv(true);
        Mbp10Schema s3 = new Mbp10Schema();

        var issues = rule.check(entry, List.of(s1, s2, s3), listing, clock);

        assertEquals(1, issues.size());
        assertTrue(issues.get(0).getDetails().contains("1 record(s) with maybeBadBook"));
        assertTrue(issues.get(0).getDetails().contains("1 record(s) with badTimestampRecv"));
        assertTrue(issues.get(0).getDetails().contains("3 total"));
    }

    @Test
    void testIssueForMbp1Schema() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_1, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp1Schema s = new Mbp1Schema();
        s.encoder.flags().maybeBadBook(true);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.BAD_DATA_FLAGS, issues.get(0).getRuleType());
    }

    @Test
    void testIssueForMboSchema() {
        MarketDataEntry entry = new MarketDataEntry(1, 2, SchemaType.MBO, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        MboSchema s = new MboSchema();
        s.encoder.flags().badTimestampRecv(true);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.BAD_DATA_FLAGS, issues.get(0).getRuleType());
    }

    @Test
    void testNoIssueForUnsupportedSchema() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.BBO_1S, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testNoIssueForEmptyRecords() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }
}
