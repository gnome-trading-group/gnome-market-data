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

public final class TimestampAlignmentRule implements QualityRule {

    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long NANOS_PER_MINUTE = 60L * NANOS_PER_SECOND;

    @Override
    public List<QualityIssue> check(MarketDataEntry entry, List<Schema> records, Listing listing, Clock clock) {
        List<QualityIssue> issues = new ArrayList<>();
        if (records.isEmpty()) {
            return issues;
        }

        LocalDateTime minuteStart = entry.getTimestamp();
        long windowStartNanos = minuteStart.toEpochSecond(ZoneOffset.UTC) * NANOS_PER_SECOND;
        long windowEndNanos = windowStartNanos + NANOS_PER_MINUTE;

        int misaligned = 0;
        for (Schema record : records) {
            long ts = record.getEventTimestamp();
            if (ts < windowStartNanos || ts >= windowEndNanos) {
                misaligned++;
            }
        }

        if (misaligned > 0) {
            QualityIssue issue = buildIssue(entry, listing, clock);
            issue.setDetails(String.format(
                    "%d of %d records have timestamps outside the expected minute window [%s, %s)",
                    misaligned, records.size(), minuteStart, minuteStart.plusMinutes(1)));
            issues.add(issue);
        }

        return issues;
    }

    private QualityIssue buildIssue(MarketDataEntry entry, Listing listing, Clock clock) {
        QualityIssue issue = new QualityIssue();
        issue.setListingId(listing.listingId());
        issue.setIssueId(
                entry.getTimestamp().toEpochSecond(ZoneOffset.UTC) + "#" + QualityRuleType.TIMESTAMP_ALIGNMENT.name());
        issue.setRuleType(QualityRuleType.TIMESTAMP_ALIGNMENT);
        issue.setStatus(QualityIssueStatus.UNREVIEWED);
        issue.setTimestamp(entry.getTimestamp());
        issue.setS3Key(entry.getKey());
        issue.setCreatedAt(LocalDateTime.now(clock));
        return issue;
    }
}
