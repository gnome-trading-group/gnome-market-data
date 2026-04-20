package group.gnometrading.quality.rules;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.model.QualityIssueStatus;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public final class BadDataFlagsRule implements QualityRule {

    @Override
    public List<QualityIssue> check(MarketDataEntry entry, List<Schema> records, Listing listing, Clock clock) {
        List<QualityIssue> issues = new ArrayList<>();
        if (!SchemaFieldExtractor.TRADE_SCHEMAS.contains(entry.getSchemaType()) || records.isEmpty()) {
            return issues;
        }

        int maybeBadBookCount = 0;
        int badTimestampRecvCount = 0;

        for (Schema record : records) {
            SchemaFieldExtractor.FlagFields flags =
                    SchemaFieldExtractor.extractFlagFields(record, entry.getSchemaType());
            if (flags.maybeBadBook()) {
                maybeBadBookCount++;
            }
            if (flags.badTimestampRecv()) {
                badTimestampRecvCount++;
            }
        }

        if (maybeBadBookCount > 0 || badTimestampRecvCount > 0) {
            QualityIssue issue = buildIssue(entry, listing, clock);
            issue.setDetails(String.format(
                    "%d record(s) with maybeBadBook flag and %d record(s) with badTimestampRecv flag out of %d total",
                    maybeBadBookCount, badTimestampRecvCount, records.size()));
            issues.add(issue);
        }

        return issues;
    }

    private QualityIssue buildIssue(MarketDataEntry entry, Listing listing, Clock clock) {
        QualityIssue issue = new QualityIssue();
        issue.setListingId(listing.listingId());
        issue.setIssueId(
                entry.getTimestamp().toEpochSecond(ZoneOffset.UTC) + "#" + QualityRuleType.BAD_DATA_FLAGS.name());
        issue.setRuleType(QualityRuleType.BAD_DATA_FLAGS);
        issue.setStatus(QualityIssueStatus.UNREVIEWED);
        issue.setTimestamp(entry.getTimestamp());
        issue.setS3Key(entry.getKey());
        issue.setCreatedAt(LocalDateTime.now(clock));
        return issue;
    }
}
