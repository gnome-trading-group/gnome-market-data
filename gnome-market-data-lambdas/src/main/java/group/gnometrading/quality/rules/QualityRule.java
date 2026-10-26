package group.gnometrading.quality.rules;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.util.List;

public interface QualityRule {
    List<QualityIssue> check(StreamEntry entry, List<StreamRecord> records, Listing listing, Clock clock);
}
