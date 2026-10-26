package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.StreamRecord;
import java.util.List;

public final class VolatilityStatistic implements QualityStatistic {

    @Override
    public String name() {
        return "volatility";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.VOLATILITY_ANOMALY;
    }

    @Override
    public AnomalyDirection anomalyDirection() {
        return AnomalyDirection.HIGH;
    }

    @Override
    public double zThreshold() {
        return 4.0;
    }

    @Override
    public double fallbackThreshold() {
        return 5.0;
    }

    @Override
    public int lookbackDays() {
        return 21;
    }

    @Override
    public double compute(StreamEntry entry, List<StreamRecord> records) {
        if (!SchemaFieldExtractor.TRADE_SCHEMAS.contains(entry.getRecordType()) || records.size() < 2) {
            return Double.NaN;
        }

        // Single-pass Welford stddev over price values
        double mean = 0;
        double m2 = 0;
        int count = 0;

        for (StreamRecord record : records) {
            SchemaFieldExtractor.TradeFields fields =
                    SchemaFieldExtractor.extractTradeFields(record, entry.getRecordType());
            if (fields.isPriceNull()) {
                continue;
            }
            double price = (double) fields.price() / Statics.PRICE_SCALING_FACTOR;
            count++;
            double delta = price - mean;
            mean += delta / count;
            m2 += delta * (price - mean);
        }

        return count < 2 ? Double.NaN : Math.sqrt(m2 / count);
    }
}
