package group.gnometrading.quality.rules.statistics;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.Statics;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class SpreadStatisticTest {

    private final SpreadStatistic statistic = new SpreadStatistic();
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);

    @Test
    void testComputesAverageSpread() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.bidPrice0(100);
        s1.encoder.askPrice0(102);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.bidPrice0(200);
        s2.encoder.askPrice0(206);

        double result = statistic.compute(entry, List.of(s1, s2));

        assertEquals(4.0 / Statics.PRICE_SCALING_FACTOR, result, 1e-12);
    }

    @Test
    void testComputesForMbp1Schema() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_1, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        group.gnometrading.schemas.Mbp1Schema s = new group.gnometrading.schemas.Mbp1Schema();
        s.encoder.bidPrice0(100);
        s.encoder.askPrice0(104);

        double result = statistic.compute(entry, List.of(s));

        assertEquals(4.0 / Statics.PRICE_SCALING_FACTOR, result, 1e-12);
    }

    @Test
    void testReturnsNanForMboSchema() {
        MarketDataEntry entry = new MarketDataEntry(1, 2, SchemaType.MBO, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        double result = statistic.compute(entry, List.of());

        assertTrue(Double.isNaN(result));
    }

    @Test
    void testSkipsNullPriceSentinels() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema nullPrices = new Mbp10Schema();
        nullPrices.encoder.bidPrice0(Mbp10Decoder.bidPrice0NullValue());
        nullPrices.encoder.askPrice0(Mbp10Decoder.askPrice0NullValue());
        Mbp10Schema valid = new Mbp10Schema();
        valid.encoder.bidPrice0(100);
        valid.encoder.askPrice0(104);

        double result = statistic.compute(entry, List.of(nullPrices, valid));

        assertEquals(4.0 / Statics.PRICE_SCALING_FACTOR, result, 1e-12);
    }

    @Test
    void testReturnsNanWhenAllNullPrices() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema nullPrices = new Mbp10Schema();
        nullPrices.encoder.bidPrice0(Mbp10Decoder.bidPrice0NullValue());
        nullPrices.encoder.askPrice0(Mbp10Decoder.askPrice0NullValue());

        double result = statistic.compute(entry, List.of(nullPrices));

        assertTrue(Double.isNaN(result));
    }

    @Test
    void testNameAndRuleType() {
        assertEquals("spread", statistic.name());
        assertEquals(QualityRuleType.SPREAD_ANOMALY, statistic.ruleType());
    }
}
