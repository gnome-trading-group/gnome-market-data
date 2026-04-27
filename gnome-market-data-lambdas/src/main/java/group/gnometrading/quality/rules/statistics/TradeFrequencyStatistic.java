package group.gnometrading.quality.rules.statistics;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Action;
import group.gnometrading.schemas.Schema;
import java.util.List;

public final class TradeFrequencyStatistic implements QualityStatistic {

    @Override
    public String name() {
        return "tradeFrequency";
    }

    @Override
    public QualityRuleType ruleType() {
        return QualityRuleType.TRADE_FREQUENCY_ANOMALY;
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
}
