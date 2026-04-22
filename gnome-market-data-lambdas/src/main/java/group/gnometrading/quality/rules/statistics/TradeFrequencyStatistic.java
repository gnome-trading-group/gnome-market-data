package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.Schema;
import java.util.List;

public final class TradeFrequencyStatistic implements QualityStatistic {

    private static final double ANOMALY_RATIO = 0.10;

    @Override
    public String name() {
        return "tradeFrequency";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.TRADE_FREQUENCY_ANOMALY;
    }

    @Override
    public double compute(MarketDataEntry entry, List<Schema> records) {
        if (!SchemaFieldExtractor.TRADE_SCHEMAS.contains(entry.getSchemaType())) {
            return Double.NaN;
        }

        int tradeCount = 0;
        for (Schema record : records) {
            SchemaFieldExtractor.TradeFields fields =
                    SchemaFieldExtractor.extractTradeFields(record, entry.getSchemaType());
            if (fields.action() == Action.Trade) {
                tradeCount++;
            }
        }
        return tradeCount;
    }

    @Override
    public boolean isAnomalous(double currentValue, double mean, double stddev) {
        return mean > 0 && currentValue < mean * ANOMALY_RATIO;
    }

    @Override
    public String describeAnomaly(double currentValue, double mean, double stddev) {
        return String.format("Trade frequency %.0f is 90%%+ below rolling average of %.1f", currentValue, mean);
    }
}
