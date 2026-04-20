package group.gnometrading.quality.rules;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityIssueStatus;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Exchange;
import group.gnometrading.sm.Listing;
import group.gnometrading.sm.Security;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TimestampAlignmentRuleTest {

    @Mock
    private Listing listing;

    @Mock
    private Exchange exchange;

    @Mock
    private Security security;

    private final TimestampAlignmentRule rule = new TimestampAlignmentRule();
    private Clock clock;
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);
    private static final long MINUTE_START_NANOS = MINUTE.toEpochSecond(ZoneOffset.UTC) * 1_000_000_000L;

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
    void testNoIssuesWhenAllTimestampsAlign() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.timestampEvent(MINUTE_START_NANOS);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.timestampEvent(MINUTE_START_NANOS + 30_000_000_000L);

        var issues = rule.check(entry, List.of(s1, s2), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testIssueWhenTimestampBeforeWindow() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.timestampEvent(MINUTE_START_NANOS - 1);

        var issues = rule.check(entry, List.of(s1), listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.TIMESTAMP_ALIGNMENT, issues.get(0).getRuleType());
        assertEquals(QualityIssueStatus.UNREVIEWED, issues.get(0).getStatus());
    }

    @Test
    void testIssueWhenTimestampAtOrAfterWindowEnd() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.timestampEvent(MINUTE_START_NANOS + 60_000_000_000L);

        var issues = rule.check(entry, List.of(s1), listing, clock);

        assertEquals(1, issues.size());
    }

    @Test
    void testIssueDetailsContainsMismatchCount() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema valid = new Mbp10Schema();
        valid.encoder.timestampEvent(MINUTE_START_NANOS);
        Mbp10Schema invalid = new Mbp10Schema();
        invalid.encoder.timestampEvent(MINUTE_START_NANOS - 1);
        Mbp10Schema invalid2 = new Mbp10Schema();
        invalid2.encoder.timestampEvent(MINUTE_START_NANOS + 60_000_000_000L);

        var issues = rule.check(entry, List.of(valid, invalid, invalid2), listing, clock);

        assertEquals(1, issues.size());
        assertTrue(issues.get(0).getDetails().contains("2 of 3"));
    }

    @Test
    void testNoIssuesForEmptyRecords() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testTimestampAtWindowEnd_ExclusiveBound() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.timestampEvent(MINUTE_START_NANOS + 59_999_999_999L);

        var issues = rule.check(entry, List.of(s1), listing, clock);

        assertTrue(issues.isEmpty());
    }
}
