package group.gnometrading.quality.rules.statistics;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.SchemaType;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class TickCountStatisticTest {

    private final TickCountStatistic statistic = new TickCountStatistic();
    private static final LocalDateTime MINUTE = LocalDateTime.of(2024, 1, 15, 10, 30);

    @Test
    void testComputeReturnsRecordCount() {
        MarketDataEntry entry =
                new MarketDataEntry(1, 2, SchemaType.MBP_10, MINUTE, MarketDataEntry.EntryType.AGGREGATED);
        List<Mbp10Schema> records = new ArrayList<>();
        for (int i = 0; i < 42; i++) records.add(new Mbp10Schema());

        assertEquals(42.0, statistic.compute(entry, new ArrayList<>(records)));
    }

    @Test
    void testAnomalousWhenCountDrops90Percent() {
        assertTrue(statistic.isAnomalous(50.0, 1000.0, 100.0));
    }

    @Test
    void testNotAnomalousWhenCountDropsLessThan90Percent() {
        assertFalse(statistic.isAnomalous(200.0, 1000.0, 100.0));
    }

    @Test
    void testNotAnomalousWhenMeanIsZero() {
        assertFalse(statistic.isAnomalous(0.0, 0.0, 0.0));
    }

    @Test
    void testNameAndRuleType() {
        assertEquals("tickCount", statistic.name());
        assertEquals(QualityRuleType.TICK_COUNT_ANOMALY, statistic.ruleType());
    }
}
