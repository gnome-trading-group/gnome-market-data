package group.gnometrading.quality.rules;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.ListingStatistics;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.model.QualityIssueStatus;
import group.gnometrading.quality.statistics.QualityStatistic;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;

public final class StatisticalQualityRule implements QualityRule {

    private final DynamoDbTable<ListingStatistics> statisticsTable;
    private final List<QualityStatistic> statistics;

    public StatisticalQualityRule(DynamoDbTable<ListingStatistics> statisticsTable, List<QualityStatistic> statistics) {
        this.statisticsTable = statisticsTable;
        this.statistics = statistics;
    }

    @Override
    public List<QualityIssue> check(MarketDataEntry entry, List<Schema> records, Listing listing, Clock clock) {
        List<QualityIssue> issues = new ArrayList<>();

        ListingStatistics listingStats = loadOrCreate(listing.listingId());

        for (QualityStatistic statistic : statistics) {
            double value = statistic.compute(entry, records);
            if (Double.isNaN(value)) {
                continue;
            }

            double mean = listingStats.getMean(statistic.name());
            double stddev = listingStats.getStdDev(statistic.name());
            int sampleCount = listingStats.getSampleCount(statistic.name());

            if (statistic.isAnomalous(value, mean, stddev, sampleCount)) {
                issues.add(buildIssue(entry, listing, clock, statistic, value, mean, stddev, records.size()));
            }

            listingStats.updateMetric(statistic.name(), value);
        }

        listingStats.setLastUpdated(LocalDateTime.now(clock));
        statisticsTable.putItem(listingStats);

        return issues;
    }

    private ListingStatistics loadOrCreate(int listingId) {
        Key key = Key.builder().partitionValue(listingId).build();
        ListingStatistics existing = statisticsTable.getItem(key);
        if (existing != null) {
            return existing;
        }
        ListingStatistics fresh = new ListingStatistics();
        fresh.setListingId(listingId);
        return fresh;
    }

    private QualityIssue buildIssue(
            MarketDataEntry entry,
            Listing listing,
            Clock clock,
            QualityStatistic statistic,
            double value,
            double mean,
            double stddev,
            int recordCount) {
        QualityIssue issue = new QualityIssue();
        issue.setListingId(listing.listingId());
        issue.setIssueId(entry.getTimestamp().toEpochSecond(ZoneOffset.UTC) + "#"
                + statistic.ruleType().name());
        issue.setRuleType(statistic.ruleType());
        issue.setStatus(QualityIssueStatus.UNREVIEWED);
        issue.setTimestamp(entry.getTimestamp());
        issue.setS3Key(entry.getKey());
        issue.setDetails(statistic.describeAnomaly(value, mean, stddev));
        issue.setRecordCount(recordCount);
        issue.setCreatedAt(LocalDateTime.now(clock));
        return issue;
    }
}
