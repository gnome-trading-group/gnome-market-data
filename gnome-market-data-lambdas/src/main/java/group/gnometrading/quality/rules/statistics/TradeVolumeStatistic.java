package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.Statics;
import java.util.List;

public final class TradeVolumeStatistic implements QualityStatistic {

    @Override
    public String name() {
        return "tradeVolume";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.TRADE_VOLUME_ANOMALY;
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
}
