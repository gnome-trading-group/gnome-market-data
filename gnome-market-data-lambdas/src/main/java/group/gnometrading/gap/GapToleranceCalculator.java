package group.gnometrading.gap;

import group.gnometrading.quality.model.HourlyListingStatistic;
import group.gnometrading.quality.rules.statistics.HourlyStatisticsAggregator;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;

public final class GapToleranceCalculator {

    static final int DEFAULT_TOLERANCE_MINUTES = 1;
    static final int MAX_TOLERANCE_MINUTES = 1440;
    static final int LOOKBACK_DAYS = 14;
    static final int MINIMUM_SAMPLES = 3;
    static final double BASE_MULTIPLIER = 3.0;

    private static final String TICK_COUNT_METRIC = "tickCount";

    private final DynamoDbTable<HourlyListingStatistic> statisticsTable;

    public GapToleranceCalculator(DynamoDbTable<HourlyListingStatistic> statisticsTable) {
        this.statisticsTable = statisticsTable;
    }

    /**
     * Returns how many consecutive missing minutes are considered normal for this listing.
     * Derived from historical tick count: sparse listings tolerate longer gaps.
     */
    public int computeToleranceMinutes(int listingId, int hour, LocalDate date) {
        if (statisticsTable == null) {
            return DEFAULT_TOLERANCE_MINUTES;
        }

        List<HourlyListingStatistic> rows = queryTickCountBaseline(listingId, hour, date);
        HourlyStatisticsAggregator.AggregatedStats stats =
                HourlyStatisticsAggregator.aggregate(rows, TICK_COUNT_METRIC);

        if (stats.sampleCount() < MINIMUM_SAMPLES) {
            return DEFAULT_TOLERANCE_MINUTES;
        }

        double mean = stats.mean();
        if (mean <= 0) {
            return MAX_TOLERANCE_MINUTES;
        }

        double multiplier = BASE_MULTIPLIER;
        double stddev = stats.stddev();
        if (stddev > 0) {
            double cv = stddev / mean;
            multiplier = Math.max(BASE_MULTIPLIER, BASE_MULTIPLIER + 2.0 * cv);
        }

        int tolerance = (int) Math.ceil((1.0 / mean) * multiplier);
        return Math.max(DEFAULT_TOLERANCE_MINUTES, Math.min(tolerance, MAX_TOLERANCE_MINUTES));
    }

    private List<HourlyListingStatistic> queryTickCountBaseline(int listingId, int hour, LocalDate date) {
        LocalDate startDate = date.minusDays(LOOKBACK_DAYS);
        String skStart = String.format("%02d#%s#%s", hour, startDate, TICK_COUNT_METRIC);
        String skEnd = String.format("%02d#%s#%s~", hour, date, TICK_COUNT_METRIC);

        QueryConditional condition = QueryConditional.sortBetween(
                Key.builder().partitionValue(listingId).sortValue(skStart).build(),
                Key.builder().partitionValue(listingId).sortValue(skEnd).build());

        List<HourlyListingStatistic> rows = new ArrayList<>();
        for (HourlyListingStatistic row : statisticsTable.query(condition).items()) {
            rows.add(row);
        }
        return rows;
    }
}
