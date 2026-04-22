package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Schema;
import java.util.List;

public final class TickCountStatistic implements QualityStatistic {

    private static final double ANOMALY_RATIO = 0.10;

    @Override
    public String name() {
        return "tickCount";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.TICK_COUNT_ANOMALY;
    }

    @Override
    public double compute(MarketDataEntry entry, List<Schema> records) {
        return records.size();
    }

    @Override
    public boolean isAnomalous(double currentValue, double mean, double stddev) {
        return mean > 0 && currentValue < mean * ANOMALY_RATIO;
    }

    @Override
    public String describeAnomaly(double currentValue, double mean, double stddev) {
        return String.format("Tick count %.0f is 90%%+ below rolling average of %.1f", currentValue, mean);
    }
}
