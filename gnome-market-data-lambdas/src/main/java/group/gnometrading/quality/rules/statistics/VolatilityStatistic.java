package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Statics;
import java.util.List;

public final class VolatilityStatistic implements QualityStatistic {

    private static final double ANOMALY_MULTIPLIER = 5.0;

    @Override
    public String name() {
        return "volatility";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.VOLATILITY_ANOMALY;
    }

    @Override
    public double compute(MarketDataEntry entry, List<Schema> records) {
        if (!SchemaFieldExtractor.TRADE_SCHEMAS.contains(entry.getSchemaType()) || records.size() < 2) {
            return Double.NaN;
        }

        // Single-pass Welford stddev over price values
        double mean = 0;
        double m2 = 0;
        int count = 0;

        for (Schema record : records) {
            SchemaFieldExtractor.TradeFields fields =
                    SchemaFieldExtractor.extractTradeFields(record, entry.getSchemaType());
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

    @Override
    public boolean isAnomalous(double currentValue, double mean, double stddev) {
        return mean > 0 && currentValue >= mean * ANOMALY_MULTIPLIER;
    }

    @Override
    public String describeAnomaly(double currentValue, double mean, double stddev) {
        return String.format(
                "Intra-minute price volatility %.2f is 5x+ above rolling average of %.2f", currentValue, mean);
    }

    @Override
    public int lookbackDays() {
        return 7;
    }
}
