package group.gnometrading.quality.rules.statistics;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.Statics;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class VolatilityStatisticTest {

    private final VolatilityStatistic statistic = new VolatilityStatistic();
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);

    @Test
    void testComputesStddevMbp10() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.price(100);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.price(200);

        double result = statistic.compute(entry, List.of(s1, s2));

        // population stddev of [100, 200] scaled by PRICE_SCALING_FACTOR = 50/1e9
        assertEquals(50.0 / Statics.PRICE_SCALING_FACTOR, result, 1e-12);
    }

    @Test
    void testComputesStddevMbp1() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_1, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp1Schema s1 = new Mbp1Schema();
        s1.encoder.price(100);
        Mbp1Schema s2 = new Mbp1Schema();
        s2.encoder.price(100);

        double result = statistic.compute(entry, List.of(s1, s2));

        assertEquals(0.0, result, 0.001);
    }

    @Test
    void testComputesStddevMbo() {
        MarketDataEntry entry = new MarketDataEntry(1, 2, SchemaType.MBO, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        MboSchema s1 = new MboSchema();
        s1.encoder.price(100);
        MboSchema s2 = new MboSchema();
        s2.encoder.price(200);
        MboSchema s3 = new MboSchema();
        s3.encoder.price(300);

        double result = statistic.compute(entry, List.of(s1, s2, s3));

        // population stddev of [100, 200, 300] scaled by PRICE_SCALING_FACTOR = ~81.65/1e9
        assertEquals(81.65 / Statics.PRICE_SCALING_FACTOR, result, 1e-12);
    }

    @Test
    void testReturnsNanForSingleRecord() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s = new Mbp10Schema();
        s.encoder.price(100);

        assertTrue(Double.isNaN(statistic.compute(entry, List.of(s))));
    }

    @Test
    void testReturnsNanForEmptyRecords() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        assertTrue(Double.isNaN(statistic.compute(entry, List.of())));
    }

    @Test
    void testReturnsNanForUnsupportedSchema() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.BBO_1S, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        assertTrue(Double.isNaN(statistic.compute(entry, List.of())));
    }

    @Test
    void testSkipsNullPriceRecords() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema nullPrice = new Mbp10Schema();
        nullPrice.encoder.price(Mbp10Decoder.priceNullValue());
        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.price(100);
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.price(200);

        double result = statistic.compute(entry, List.of(nullPrice, s1, s2));

        // null price record is skipped; stddev of [100, 200] scaled = 50/1e9
        assertEquals(50.0 / Statics.PRICE_SCALING_FACTOR, result, 1e-12);
    }

    @Test
    void testReturnsNanWhenOnlyNullPrices() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);

        Mbp10Schema s1 = new Mbp10Schema();
        s1.encoder.price(Mbp10Decoder.priceNullValue());
        Mbp10Schema s2 = new Mbp10Schema();
        s2.encoder.price(Mbp10Decoder.priceNullValue());

        assertTrue(Double.isNaN(statistic.compute(entry, List.of(s1, s2))));
    }

    @Test
    void testDetectionEnabled() {
        assertFalse(statistic.detectionEnabled());
    }

    @Test
    void testNameAndRuleType() {
        assertEquals("volatility", statistic.name());
        assertEquals(QualityRuleType.VOLATILITY_ANOMALY, statistic.ruleType());
    }
}
