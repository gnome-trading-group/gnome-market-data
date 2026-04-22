package group.gnometrading.quality.rules.statistics;

import static org.junit.jupiter.api.Assertions.*;

import group.gnometrading.quality.model.HourlyListingStatistic;
import java.util.List;
import org.junit.jupiter.api.Test;

class HourlyStatisticsAggregatorTest {

    @Test
    void testEmptyListReturnsZeroStats() {
        HourlyStatisticsAggregator.AggregatedStats result =
                HourlyStatisticsAggregator.aggregate(List.of(), "tickCount");

        assertEquals(0, result.sampleCount());
        assertEquals(0.0, result.mean(), 1e-12);
        assertEquals(0.0, result.stddev(), 1e-12);
    }

    @Test
    void testRowWithNonMatchingMetricSkipped() {
        HourlyListingStatistic row = buildStat(14, "2024-01-01", "spread", 1.0, 5.0, 25.0);

        HourlyStatisticsAggregator.AggregatedStats result =
                HourlyStatisticsAggregator.aggregate(List.of(row), "tickCount");

        assertEquals(0, result.sampleCount());
    }

    @Test
    void testSingleRowSingleObservation() {
        HourlyListingStatistic row = buildStat(14, "2024-01-01", "tickCount", 1.0, 100.0, 10000.0);

        HourlyStatisticsAggregator.AggregatedStats result =
                HourlyStatisticsAggregator.aggregate(List.of(row), "tickCount");

        assertEquals(1, result.sampleCount());
        assertEquals(100.0, result.mean(), 1e-12);
        assertEquals(0.0, result.stddev(), 1e-12);
    }

    @Test
    void testAggregationAcrossMultipleDays() {
        // day1: two observations 100 and 200 → count=2, sum=300, sos=50000
        HourlyListingStatistic day1 = buildStat(14, "2024-01-01", "tickCount", 2.0, 300.0, 50000.0);
        // day2: two observations 150 and 250 → count=2, sum=400, sos=85000
        HourlyListingStatistic day2 = buildStat(14, "2024-01-02", "tickCount", 2.0, 400.0, 85000.0);

        HourlyStatisticsAggregator.AggregatedStats result =
                HourlyStatisticsAggregator.aggregate(List.of(day1, day2), "tickCount");

        assertEquals(4, result.sampleCount());
        assertEquals(175.0, result.mean(), 1e-12);
        // population stddev of {100, 200, 150, 250}: mean=175, variance=3125
        assertEquals(Math.sqrt(3125.0), result.stddev(), 1e-9);
    }

    @Test
    void testMeanAndStddevWithKnownValues() {
        // Values: 10, 20, 30 — mean=20, population variance=200/3≈66.67, stddev≈8.165
        // count=3, sum=60, sumOfSquares=1400
        HourlyListingStatistic row = buildStat(0, "2024-01-01", "spread", 3.0, 60.0, 1400.0);

        HourlyStatisticsAggregator.AggregatedStats result =
                HourlyStatisticsAggregator.aggregate(List.of(row), "spread");

        assertEquals(3, result.sampleCount());
        assertEquals(20.0, result.mean(), 1e-9);
        assertEquals(Math.sqrt(200.0 / 3.0), result.stddev(), 1e-9);
    }

    @Test
    void testOnlyAggregatesMatchingMetric() {
        HourlyListingStatistic tickRow = buildStat(9, "2024-01-01", "tickCount", 1.0, 50.0, 2500.0);
        HourlyListingStatistic spreadRow1 = buildStat(14, "2024-01-01", "spread", 1.0, 200.0, 40000.0);
        HourlyListingStatistic spreadRow2 = buildStat(14, "2024-01-02", "spread", 1.0, 300.0, 90000.0);

        HourlyStatisticsAggregator.AggregatedStats result =
                HourlyStatisticsAggregator.aggregate(List.of(tickRow, spreadRow1, spreadRow2), "spread");

        assertEquals(2, result.sampleCount());
        assertEquals(250.0, result.mean(), 1e-12);
    }

    private HourlyListingStatistic buildStat(
            int hour, String date, String metric, double count, double sum, double sumOfSquares) {
        HourlyListingStatistic stat = new HourlyListingStatistic();
        stat.setListingId(42);
        stat.setSk(HourlyListingStatistic.buildSk(hour, date, metric));
        stat.setCount(count);
        stat.setSum(sum);
        stat.setSumOfSquares(sumOfSquares);
        return stat;
    }
}
