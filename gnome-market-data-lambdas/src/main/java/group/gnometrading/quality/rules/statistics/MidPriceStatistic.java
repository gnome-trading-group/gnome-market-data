package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Statics;
import java.util.List;

public final class MidPriceStatistic implements QualityStatistic {

    private static final double ANOMALY_Z_SCORE = 4.0;

    @Override
    public String name() {
        return "midPrice";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.MID_PRICE_ANOMALY;
    }

    @Override
    public double compute(MarketDataEntry entry, List<Schema> records) {
        if (!SchemaFieldExtractor.BBO_SCHEMAS.contains(entry.getSchemaType()) || records.isEmpty()) {
            return Double.NaN;
        }

        long totalMid = 0;
        int validCount = 0;

        for (Schema record : records) {
            SchemaFieldExtractor.BboFields bbo = SchemaFieldExtractor.extractBboFields(record, entry.getSchemaType());
            if (!bbo.isBidPriceNull() && !bbo.isAskPriceNull()) {
                totalMid += bbo.bidPrice() + bbo.askPrice();
                validCount++;
            }
        }

        return validCount == 0 ? Double.NaN : (double) totalMid / (2.0 * validCount) / Statics.PRICE_SCALING_FACTOR;
    }

    @Override
    public boolean isAnomalous(double currentValue, double mean, double stddev) {
        if (stddev == 0) {
            return false;
        }
        return Math.abs(currentValue - mean) > ANOMALY_Z_SCORE * stddev;
    }

    @Override
    public String describeAnomaly(double currentValue, double mean, double stddev) {
        double zScore = stddev > 0 ? Math.abs(currentValue - mean) / stddev : 0;
        return String.format(
                "Mid-price %.2f is %.1fσ from rolling average of %.2f (stddev=%.2f)",
                currentValue, zScore, mean, stddev);
    }

    @Override
    public int lookbackDays() {
        return 7;
    }
}
