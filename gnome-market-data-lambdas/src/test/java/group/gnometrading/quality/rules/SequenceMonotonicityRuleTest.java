package group.gnometrading.quality.rules;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Mbp10Schema;
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
class SequenceMonotonicityRuleTest {

    @Mock
    private Listing listing;

    @Mock
    private Exchange exchange;

    @Mock
    private Security security;

    private final SequenceMonotonicityRule rule = new SequenceMonotonicityRule();
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
    void testNoIssuesForStrictlyIncreasing() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(entry, List.of(schema(1), schema(2), schema(3), schema(100)), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testNoIssueForDuplicate() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(entry, List.of(schema(1), schema(2), schema(2), schema(3)), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testIssueWhenAllOutOfOrder() {
        // 1/1 transitions = 100% > 20% threshold
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(entry, List.of(schema(5), schema(4)), listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.SEQUENCE_MONOTONICITY, issues.get(0).getRuleType());
        assertTrue(issues.get(0).getDetails().contains("1 decrease"));
    }

    @Test
    void testIssueDetailsIncludesPercentage() {
        // [1, 1, 5, 3, 10]: 1 decrease out of 4 transitions = 25% > 20%
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(entry, List.of(schema(1), schema(1), schema(5), schema(3), schema(10)), listing, clock);

        assertEquals(1, issues.size());
        assertTrue(issues.get(0).getDetails().contains("1 decrease"));
        assertTrue(issues.get(0).getDetails().contains("4 transitions"));
        assertTrue(issues.get(0).getDetails().contains("5 records"));
    }

    @Test
    void testNoIssueForMinorJitter() {
        // 1 decrease out of 10 transitions = 10% — under the 20% threshold
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(
                entry,
                List.of(
                        schema(1),
                        schema(2),
                        schema(3),
                        schema(4),
                        schema(5),
                        schema(6),
                        schema(5),
                        schema(8),
                        schema(9),
                        schema(10),
                        schema(11)),
                listing,
                clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testIssueAtBoundary() {
        // 2 decreases out of 9 transitions = 22.2% — just over the 20% threshold
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(
                entry,
                List.of(
                        schema(1),
                        schema(2),
                        schema(1),
                        schema(4),
                        schema(5),
                        schema(6),
                        schema(5),
                        schema(8),
                        schema(9),
                        schema(10)),
                listing,
                clock);

        assertEquals(1, issues.size());
    }

    @Test
    void testNoIssueForSingleRecord() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(entry, List.of(schema(1)), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testNoIssueForEmptyRecords() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        var issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }

    private Mbp10Schema schema(long sequence) {
        Mbp10Schema s = new Mbp10Schema();
        s.encoder.sequence(sequence);
        return s;
    }
}
