package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Statics;
import java.util.List;

public final class MidPriceStatistic implements QualityStatistic {

    @Override
    public String name() {
        return "midPrice";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.MID_PRICE_ANOMALY;
    }

    @Override
    public AnomalyDirection anomalyDirection() {
        return AnomalyDirection.BOTH;
    }

    @Override
    public double zThreshold() {
        return 4.0;
    }

    @Override
    public double fallbackThreshold() {
        return 0.0;
    }

    @Override
    public int hourWindow() {
        return 0;
    }

    @Override
    public int lookbackDays() {
        return 21;
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
}
