package group.gnometrading.quality.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Statics;
import java.util.List;

public final class TradeVolumeStatistic implements QualityStatistic {

    private static final int WARMUP_SAMPLES = 30;
    private static final double ANOMALY_RATIO = 0.10;

    @Override
    public String name() {
        return "tradeVolume";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.TRADE_VOLUME_ANOMALY;
    }

    @Override
    public double compute(MarketDataEntry entry, List<Schema> records) {
        if (!SchemaFieldExtractor.TRADE_SCHEMAS.contains(entry.getSchemaType())) {
            return Double.NaN;
        }

        long totalVolume = 0;
        for (Schema record : records) {
            SchemaFieldExtractor.TradeFields fields =
                    SchemaFieldExtractor.extractTradeFields(record, entry.getSchemaType());
            if (fields.action() == Action.Trade) {
                totalVolume += fields.size();
            }
        }
        return (double) totalVolume / Statics.SIZE_SCALING_FACTOR;
    }

    @Override
    public boolean isAnomalous(double currentValue, double mean, double stddev, int sampleCount) {
        return sampleCount >= WARMUP_SAMPLES && mean > 0 && currentValue < mean * ANOMALY_RATIO;
    }

    @Override
    public String describeAnomaly(double currentValue, double mean, double stddev) {
        return String.format("Trade volume %.0f is 90%%+ below rolling average of %.1f", currentValue, mean);
    }
}
