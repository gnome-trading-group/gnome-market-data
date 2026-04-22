package group.gnometrading.quality.rules.statistics;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.SchemaType;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class TradeFrequencyStatisticTest {

    private final TradeFrequencyStatistic statistic = new TradeFrequencyStatistic();
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);

    @Test
    void testCountsTradesMbp10() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema trade1 = new Mbp10Schema();
        trade1.encoder.action(Action.Trade);
        Mbp10Schema trade2 = new Mbp10Schema();
        trade2.encoder.action(Action.Trade);
        Mbp10Schema nonTrade = new Mbp10Schema();
        nonTrade.encoder.action(Action.Add);

        double result = statistic.compute(entry, List.of(trade1, trade2, nonTrade));

        assertEquals(2.0, result, 0.001);
    }

    @Test
    void testCountsTradesMbp1() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_1, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp1Schema trade = new Mbp1Schema();
        trade.encoder.action(Action.Trade);
        Mbp1Schema nonTrade = new Mbp1Schema();
        nonTrade.encoder.action(Action.Modify);

        double result = statistic.compute(entry, List.of(trade, nonTrade));

        assertEquals(1.0, result, 0.001);
    }

    @Test
    void testCountsTradesMbo() {
        MarketDataEntry entry = new MarketDataEntry(1, 2, SchemaType.MBO, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        MboSchema trade1 = new MboSchema();
        trade1.encoder.action(Action.Trade);
        MboSchema trade2 = new MboSchema();
        trade2.encoder.action(Action.Fill);

        double result = statistic.compute(entry, List.of(trade1, trade2));

        assertEquals(1.0, result, 0.001);
    }

    @Test
    void testReturnsZeroWhenNoTrades() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema nonTrade = new Mbp10Schema();
        nonTrade.encoder.action(Action.Cancel);

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
    void testAnomalousWhenFrequencyDrops90Percent() {
        assertTrue(statistic.isAnomalous(50.0, 1000.0, 100.0));
    }

    @Test
    void testNotAnomalousWhenFrequencyDropsLessThan90Percent() {
        assertFalse(statistic.isAnomalous(200.0, 1000.0, 100.0));
    }

    @Test
    void testNotAnomalousWhenMeanIsZero() {
        assertFalse(statistic.isAnomalous(0.0, 0.0, 0.0));
    }

    @Test
    void testNameAndRuleType() {
        assertEquals("tradeFrequency", statistic.name());
        assertEquals(QualityRuleType.TRADE_FREQUENCY_ANOMALY, statistic.ruleType());
    }
}
