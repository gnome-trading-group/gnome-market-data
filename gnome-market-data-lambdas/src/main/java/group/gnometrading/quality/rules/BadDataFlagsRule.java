package group.gnometrading.quality.rules;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.model.QualityIssueStatus;
import group.gnometrading.quality.model.QualityRuleType;
import group.gnometrading.quality.utils.SchemaFieldExtractor;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public final class BadDataFlagsRule implements QualityRule {

    @Override
    public List<QualityIssue> check(StreamEntry entry, List<StreamRecord> records, Listing listing, Clock clock) {
        List<QualityIssue> issues = new ArrayList<>();
        if (!SchemaFieldExtractor.TRADE_SCHEMAS.contains(entry.getRecordType()) || records.isEmpty()) {
            return issues;
        }

        int badDataCount = 0;
        int badTimestampRecvCount = 0;

        for (StreamRecord record : records) {
            SchemaFieldExtractor.FlagFields flags =
                    SchemaFieldExtractor.extractFlagFields(record, entry.getRecordType());
            if (flags.badData()) {
                badDataCount++;
            }
            if (flags.badTimestampRecv()) {
                badTimestampRecvCount++;
            }
        }

        if (badDataCount > 0 || badTimestampRecvCount > 0) {
            QualityIssue issue = buildIssue(entry, listing, clock);
            issue.setDetails(String.format(
                    "%d record(s) with badData flag and %d record(s) with badTimestampRecv flag out of %d total",
                    badDataCount, badTimestampRecvCount, records.size()));
            issues.add(issue);
        }

        return issues;
    }

    private QualityIssue buildIssue(StreamEntry entry, Listing listing, Clock clock) {
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
