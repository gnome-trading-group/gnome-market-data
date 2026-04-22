package group.gnometrading.quality.rules.statistics;

import group.gnometrading.quality.model.HourlyListingStatistic;
import java.util.List;

public final class HourlyStatisticsAggregator {

    private HourlyStatisticsAggregator() {}

    /**
     * Aggregates sufficient statistics for a given metric across a list of hourly rows.
     * The rows are already pre-filtered to the relevant hour by the query; this method
     * sums across all rows whose SK encodes the requested metric name.
     */
    public static AggregatedStats aggregate(List<HourlyListingStatistic> rows, String metricName) {
        double totalCount = 0;
        double totalSum = 0;
        double totalSumOfSquares = 0;

        for (HourlyListingStatistic row : rows) {
            if (!metricName.equals(row.getMetric())) {
                continue;
            }
            if (row.getCount() != null) {
                totalCount += row.getCount();
            }
            if (row.getSum() != null) {
                totalSum += row.getSum();
            }
            if (row.getSumOfSquares() != null) {
                totalSumOfSquares += row.getSumOfSquares();
            }
        }

        return new AggregatedStats(totalCount, totalSum, totalSumOfSquares);
    }

    public record AggregatedStats(double count, double sum, double sumOfSquares) {
        public double mean() {
            return count > 0 ? sum / count : 0.0;
        }

        public double stddev() {
            if (count < 2) {
                return 0.0;
            }
            double variance = (sumOfSquares / count) - (sum / count) * (sum / count);
            return Math.sqrt(Math.max(0.0, variance));
        }

        public int sampleCount() {
            return (int) count;
        }
    }
}
