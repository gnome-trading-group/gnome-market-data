package group.gnometrading.quality.rules.statistics;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.book.Action;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class TradeVolumeStatisticTest {

    private final TradeVolumeStatistic statistic = new TradeVolumeStatistic();
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);

    @Test
    void testSumsTradeVolumeMbp10() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBP_10, MINUTE, StreamEntry.EntryType.AGGREGATED);

        Mbp10Schema trade1 = new Mbp10Schema();
        trade1.encoder.action(Action.Trade);
        trade1.encoder.size(100);
        Mbp10Schema trade2 = new Mbp10Schema();
        trade2.encoder.action(Action.Trade);
        trade2.encoder.size(50);
        Mbp10Schema nonTrade = new Mbp10Schema();
        nonTrade.encoder.action(Action.Add);
        nonTrade.encoder.size(200);

        double result = statistic.compute(entry, List.of(trade1, trade2, nonTrade));

        assertEquals(150.0 / Statics.SIZE_SCALING_FACTOR, result, 1e-9);
    }

    @Test
    void testSumsTradeVolumeMbp1() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBP_1, MINUTE, StreamEntry.EntryType.AGGREGATED);

        Mbp1Schema trade = new Mbp1Schema();
        trade.encoder.action(Action.Trade);
        trade.encoder.size(75);

        double result = statistic.compute(entry, List.of(trade));

        assertEquals(75.0 / Statics.SIZE_SCALING_FACTOR, result, 1e-9);
    }

    @Test
    void testSumsTradeVolumeMbo() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBO, MINUTE, StreamEntry.EntryType.AGGREGATED);

        MboSchema trade = new MboSchema();
        trade.encoder.action(Action.Trade);
        trade.encoder.size(30);

        double result = statistic.compute(entry, List.of(trade));

        assertEquals(30.0 / Statics.SIZE_SCALING_FACTOR, result, 1e-9);
    }

    @Test
    void testReturnsZeroWhenNoTrades() {
        StreamEntry entry = new StreamEntry(1, RecordType.MBP_10, MINUTE, StreamEntry.EntryType.AGGREGATED);

        Mbp10Schema nonTrade = new Mbp10Schema();
        nonTrade.encoder.action(Action.Add);
        nonTrade.encoder.size(500);

        double result = statistic.compute(entry, List.of(nonTrade));

        assertEquals(0.0, result, 0.001);
    }

    @Test
    void testReturnsNanForUnsupportedSchema() {
        StreamEntry entry = new StreamEntry(1, RecordType.BBO_1S, MINUTE, StreamEntry.EntryType.AGGREGATED);

        assertTrue(Double.isNaN(statistic.compute(entry, List.of())));
    }

    @Test
    void testDetectionEnabled() {
        assertFalse(statistic.detectionEnabled());
    }

    @Test
    void testNameAndRuleType() {
        assertEquals("tradeVolume", statistic.name());
        assertEquals(QualityRuleType.TRADE_VOLUME_ANOMALY, statistic.ruleType());
    }
}
