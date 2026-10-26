package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.StreamRecord;
import java.util.List;

public final class TickCountStatistic implements QualityStatistic {

    @Override
    public String name() {
        return "tickCount";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.TICK_COUNT_ANOMALY;
    }

    @Override
    public AnomalyDirection anomalyDirection() {
        return AnomalyDirection.LOW;
    }

    @Override
    public double zThreshold() {
        return 3.0;
    }

    @Override
    public double fallbackThreshold() {
        return 0.10;
    }

    @Override
    public int lookbackDays() {
        return 28;
    }

    @Override
    public boolean detectionEnabled() {
        return true;
    }

    @Override
    public double compute(StreamEntry entry, List<StreamRecord> records) {
        return records.size();
    }
}
