package group.gnometrading.quality.rules;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.model.QualityIssueStatus;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public final class SequenceMonotonicityRule implements QualityRule {

    private static final double ANOMALY_RATIO = 0.20;

    @Override
    public List<QualityIssue> check(MarketDataEntry entry, List<Schema> records, Listing listing, Clock clock) {
        List<QualityIssue> issues = new ArrayList<>();
        if (records.size() < 2) {
            return issues;
        }

        int decreases = 0;
        long previous = records.get(0).getSequenceNumber();

        for (int i = 1; i < records.size(); i++) {
            long current = records.get(i).getSequenceNumber();
            if (current < previous) {
                decreases++;
            }
            previous = current;
        }

        int transitions = records.size() - 1;
        if ((double) decreases / transitions > ANOMALY_RATIO) {
            QualityIssue issue = buildIssue(entry, listing, clock);
            issue.setDetails(String.format(
                    "%d decrease(s) out of %d transitions (%.1f%%) in %d records",
                    decreases, transitions, (double) decreases / transitions * 100, records.size()));
            issues.add(issue);
        }

        return issues;
    }

    private QualityIssue buildIssue(MarketDataEntry entry, Listing listing, Clock clock) {
        QualityIssue issue = new QualityIssue();
        issue.setListingId(listing.listingId());
        issue.setIssueId(entry.getTimestamp().toEpochSecond(ZoneOffset.UTC) + "#"
                + QualityRuleType.SEQUENCE_MONOTONICITY.name());
        issue.setRuleType(QualityRuleType.SEQUENCE_MONOTONICITY);
        issue.setStatus(QualityIssueStatus.UNREVIEWED);
        issue.setTimestamp(entry.getTimestamp());
        issue.setS3Key(entry.getKey());
        issue.setCreatedAt(LocalDateTime.now(clock));
        return issue;
    }
}
