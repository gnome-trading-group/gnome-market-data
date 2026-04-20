package group.gnometrading.quality.rules;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.util.List;

public interface QualityRule {
    List<QualityIssue> check(MarketDataEntry entry, List<Schema> records, Listing listing, Clock clock);
}
