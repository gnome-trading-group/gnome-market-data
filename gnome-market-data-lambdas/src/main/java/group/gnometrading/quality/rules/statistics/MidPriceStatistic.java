package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Statics;
import group.gnometrading.schemas.StreamRecord;
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
    public double compute(StreamEntry entry, List<StreamRecord> records) {
        if (!SchemaFieldExtractor.BBO_SCHEMAS.contains(entry.getRecordType()) || records.isEmpty()) {
            return Double.NaN;
        }

        // A long overflows once ~46k BTC-scale (1e14) bid+ask pairs are summed in one minute.
        double totalMid = 0;
        int validCount = 0;

        for (StreamRecord record : records) {
            SchemaFieldExtractor.BboFields bbo = SchemaFieldExtractor.extractBboFields(record, entry.getRecordType());
            if (!bbo.isBidPriceNull() && !bbo.isAskPriceNull()) {
                totalMid += (double) bbo.bidPrice() + bbo.askPrice();
                validCount++;
            }
        }

        return validCount == 0 ? Double.NaN : totalMid / (2.0 * validCount) / Statics.PRICE_SCALING_FACTOR;
    }
}
