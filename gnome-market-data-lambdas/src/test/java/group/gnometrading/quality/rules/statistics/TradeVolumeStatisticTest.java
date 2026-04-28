package group.gnometrading.quality.rules.statistics;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.Statics;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class TradeVolumeStatisticTest {

    private final TradeVolumeStatistic statistic = new TradeVolumeStatistic();
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);

    @Test
    void testSumsTradeVolumeMbp10() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

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
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_1, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp1Schema trade = new Mbp1Schema();
        trade.encoder.action(Action.Trade);
        trade.encoder.size(75);

        double result = statistic.compute(entry, List.of(trade));

        assertEquals(75.0 / Statics.SIZE_SCALING_FACTOR, result, 1e-9);
    }

    @Test
    void testSumsTradeVolumeMbo() {
        MarketDataEntry entry = new MarketDataEntry(1, 2, SchemaType.MBO, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        MboSchema trade = new MboSchema();
        trade.encoder.action(Action.Trade);
        trade.encoder.size(30);

        double result = statistic.compute(entry, List.of(trade));

        assertEquals(30.0 / Statics.SIZE_SCALING_FACTOR, result, 1e-9);
    }

    @Test
    void testReturnsZeroWhenNoTrades() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema nonTrade = new Mbp10Schema();
        nonTrade.encoder.action(Action.Add);
        nonTrade.encoder.size(500);

        double result = statistic.compute(entry, List.of(nonTrade));

        assertEquals(0.0, result, 0.001);
    }

    @Test
    void testReturnsNanForUnsupportedSchema() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.BBO_1S, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

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
