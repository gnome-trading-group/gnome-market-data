package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Statics;
import java.util.List;

public final class SpreadStatistic implements QualityStatistic {

    private static final double ANOMALY_MULTIPLIER = 10.0;

    @Override
    public String name() {
        return "spread";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.SPREAD_ANOMALY;
    }

    @Override
    public double compute(MarketDataEntry entry, List<Schema> records) {
        if (!SchemaFieldExtractor.BBO_SCHEMAS.contains(entry.getSchemaType()) || records.isEmpty()) {
            return Double.NaN;
        }

        long totalSpread = 0;
        int validCount = 0;

        for (Schema record : records) {
            SchemaFieldExtractor.BboFields bbo = SchemaFieldExtractor.extractBboFields(record, entry.getSchemaType());
            if (!bbo.isBidPriceNull() && !bbo.isAskPriceNull() && bbo.askPrice() >= bbo.bidPrice()) {
                totalSpread += bbo.askPrice() - bbo.bidPrice();
                validCount++;
            }
        }

        return validCount == 0 ? Double.NaN : (double) totalSpread / validCount / Statics.PRICE_SCALING_FACTOR;
    }

    @Override
    public boolean isAnomalous(double currentValue, double mean, double stddev) {
        return mean > 0 && currentValue >= mean * ANOMALY_MULTIPLIER;
    }

    @Override
    public String describeAnomaly(double currentValue, double mean, double stddev) {
        return String.format("Average spread %.2f is 10x+ above rolling average of %.2f", currentValue, mean);
    }

    @Override
    public int lookbackDays() {
        return 7;
    }
}
