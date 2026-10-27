package group.gnometrading.quality.rules;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.RecordType;
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
        lenient().when(security.securityId()).thenReturn(2);
    }

    @Test
    void testNoIssuesWhenNoFlagsSet() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBP_10, MINUTE);

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.flags().badData(false);
        s.encoder.flags().badTimestampRecv(false);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testIssueForMaybeBadBookFlag() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBP_10, MINUTE);

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.flags().badData(true);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.BAD_DATA_FLAGS, issues.get(0).getRuleType());
        assertTrue(issues.get(0).getDetails().contains("1 record(s) with badData"));
    }

    @Test
    void testIssueForBadTimestampRecvFlag() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBP_10, MINUTE);

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.flags().badTimestampRecv(true);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertEquals(1, issues.size());
        assertTrue(issues.get(0).getDetails().contains("1 record(s) with badTimestampRecv"));
    }

    @Test
    void testIssueDetailsCountsBothFlagTypes() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBP_10, MINUTE);

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.flags().badData(true);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.flags().badTimestampRecv(true);
        Mbp10Schema s3 = new Mbp10Schema();

        var issues = rule.check(entry, List.of(s1, s2, s3), listing, clock);

        assertEquals(1, issues.size());
        assertTrue(issues.get(0).getDetails().contains("1 record(s) with badData"));
        assertTrue(issues.get(0).getDetails().contains("1 record(s) with badTimestampRecv"));
        assertTrue(issues.get(0).getDetails().contains("3 total"));
    }

    @Test
    void testIssueForMbp1Schema() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBP_1, MINUTE);

        Mbp1Schema s = new Mbp1Schema();
        s.encoder.flags().badData(true);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.BAD_DATA_FLAGS, issues.get(0).getRuleType());
    }

    @Test
    void testIssueForMboSchema() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBO, MINUTE);

        MboSchema s = new MboSchema();
        s.encoder.flags().badTimestampRecv(true);

        var issues = rule.check(entry, List.of(s), listing, clock);

        assertEquals(1, issues.size());
        assertEquals(QualityRuleType.BAD_DATA_FLAGS, issues.get(0).getRuleType());
    }

    @Test
    void testNoIssueForUnsupportedSchema() {
        StreamEntry entry = new StreamEntry(1, RecordType.BBO_1S, MINUTE);

        var issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }

    @Test
    void testNoIssueForEmptyRecords() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBP_10, MINUTE);

        var issues = rule.check(entry, List.of(), listing, clock);

        assertTrue(issues.isEmpty());
    }
}
