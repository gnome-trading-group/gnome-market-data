package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Schema;
import java.util.List;

public interface QualityStatistic {
    String name();

    QualityRuleType ruleType();

    /**
     * Computes a scalar metric from the given records.
     * Return Double.NaN to signal that this statistic is not applicable for the given entry
     * (e.g., a schema-specific metric on a non-matching schema type). NaN values are skipped
     * by StatisticalQualityRule — no issue is created and the baseline is not updated.
     */
    double compute(MarketDataEntry entry, List<Schema> records);

    AnomalyDirection anomalyDirection();

    /** Z-score threshold above which the observation is flagged as anomalous. */
    double zThreshold();

    /**
     * Fallback threshold used when stddev=0 (perfectly stable baseline).
     * For LOW: flags when value < mean * fallback.
     * For HIGH: flags when value >= mean * fallback.
     * For BOTH: ignored — stddev=0 always returns false.
     */
    double fallbackThreshold();

    /** Hours +/- around the entry hour to include in the conditioning window. 0 = exact hour only. */
    default int hourWindow() {
        return 1;
    }

    /** Calendar days of history to include in the baseline. */
    default int lookbackDays() {
        return 14;
    }

    /**
     * Minimum number of distinct same-day-type days of historical data required before
     * anomaly detection activates.
     */
    default int minimumDays() {
        return 3;
    }

    /**
     * Maximum calendar days since the most recent same-day-type observation before detection
     * is suppressed to force re-warm after a data gap. Defaults to 3x minimumDays to allow
     * for the 7-day gap between same-type weekend days.
     */
    default int freshnessDays() {
        return minimumDays() * 3;
    }

    default boolean detectionEnabled() {
        return false;
    }

    default boolean isAnomalous(double currentValue, double mean, double stddev) {
        if (mean <= 0) {
            return false;
        }
        if (stddev > 0) {
            double zScore =
                    switch (anomalyDirection()) {
                        case LOW -> (mean - currentValue) / stddev;
                        case HIGH -> (currentValue - mean) / stddev;
                        case BOTH -> Math.abs(currentValue - mean) / stddev;
                    };
            return zScore > zThreshold();
        }
        return switch (anomalyDirection()) {
            case LOW -> currentValue < mean * fallbackThreshold();
            case HIGH -> currentValue >= mean * fallbackThreshold();
            case BOTH -> false;
        };
    }

    default String describeAnomaly(double currentValue, double mean, double stddev) {
        String direction =
                switch (anomalyDirection()) {
                    case LOW -> "below";
                    case HIGH -> "above";
                    case BOTH -> currentValue >= mean ? "above" : "below";
                };
        if (stddev > 0) {
            double zScore = Math.abs(currentValue - mean) / stddev;
            return String.format(
                    "%s %.4g is %.1fσ %s conditional mean of %.4g (σ=%.4g)",
                    name(), currentValue, zScore, direction, mean, stddev);
        }
        return String.format(
                "%s %.4g is anomalously %s conditional mean of %.4g (zero variance baseline)",
                name(), currentValue, direction, mean);
    }
}
