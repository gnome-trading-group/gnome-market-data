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

    /**
     * Returns true if the current value is anomalous relative to the rolling baseline.
     * Warmup and freshness checks are handled by StatisticalQualityRule before calling this.
     */
    boolean isAnomalous(double currentValue, double mean, double stddev);

    String describeAnomaly(double currentValue, double mean, double stddev);

    /** How many days of history to include in the baseline. */
    default int lookbackDays() {
        return 14;
    }

    /**
     * Minimum number of distinct days of historical data required before anomaly detection
     * activates. Also used as the gap-freshness threshold — if the most recent historical data
     * point for this metric+hour is older than minimumDays days, detection is suppressed to
     * force re-warm after a data gap.
     */
    default int minimumDays() {
        return 3;
    }
}
