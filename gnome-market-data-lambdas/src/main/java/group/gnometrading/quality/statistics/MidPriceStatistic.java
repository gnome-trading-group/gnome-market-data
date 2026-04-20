package group.gnometrading.quality.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Statics;
import java.util.List;

public final class MidPriceStatistic implements QualityStatistic {

    private static final int WARMUP_SAMPLES = 30;
    private static final double ANOMALY_DEVIATION_RATIO = 0.10;

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
    public boolean isAnomalous(double currentValue, double mean, double stddev, int sampleCount) {
        if (sampleCount < WARMUP_SAMPLES || mean == 0) {
            return false;
        }
        return Math.abs(currentValue - mean) / mean > ANOMALY_DEVIATION_RATIO;
    }

    @Override
    public String describeAnomaly(double currentValue, double mean, double stddev) {
        double pctChange = mean == 0 ? 0 : Math.abs(currentValue - mean) / mean * 100;
        return String.format(
                "Mid-price %.2f deviates %.1f%% from rolling average of %.2f", currentValue, pctChange, mean);
    }
}
