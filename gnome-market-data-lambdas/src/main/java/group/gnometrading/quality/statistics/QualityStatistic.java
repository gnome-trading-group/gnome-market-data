package group.gnometrading.quality.statistics;

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
     * Implementations should return false when sampleCount is below a warmup threshold
     * to avoid false positives on cold starts.
     */
    boolean isAnomalous(double currentValue, double mean, double stddev, int sampleCount);

    String describeAnomaly(double currentValue, double mean, double stddev);
}
