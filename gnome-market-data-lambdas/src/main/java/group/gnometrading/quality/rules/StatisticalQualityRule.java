package group.gnometrading.quality.rules;

import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.HourlyListingStatistic;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.model.QualityIssueStatus;
import group.gnometrading.quality.rules.statistics.HourlyStatisticsAggregator;
import group.gnometrading.quality.rules.statistics.QualityStatistic;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public final class StatisticalQualityRule implements QualityRule {

    private final DynamoDbTable<HourlyListingStatistic> statisticsTable;
    private final DynamoDbClient dynamoDbClient;
    private final String tableName;
    private final List<QualityStatistic> statistics;
    private final int maxLookback;
    private final boolean updateStatistics;
    private final boolean detectAnomalies;

    public StatisticalQualityRule(
            DynamoDbTable<HourlyListingStatistic> statisticsTable,
            DynamoDbClient dynamoDbClient,
            String tableName,
            List<QualityStatistic> statistics) {
        this(statisticsTable, dynamoDbClient, tableName, statistics, true, true);
    }

    public StatisticalQualityRule(
            DynamoDbTable<HourlyListingStatistic> statisticsTable,
            DynamoDbClient dynamoDbClient,
            String tableName,
            List<QualityStatistic> statistics,
            boolean updateStatistics,
            boolean detectAnomalies) {
        this.statisticsTable = statisticsTable;
        this.dynamoDbClient = dynamoDbClient;
        this.tableName = tableName;
        this.statistics = statistics;
        this.updateStatistics = updateStatistics;
        this.detectAnomalies = detectAnomalies;
        this.maxLookback = statistics.stream()
                .mapToInt(QualityStatistic::lookbackDays)
                .max()
                .orElse(14);
    }

    private record Warmup(int distinctDays, LocalDate latestDate) {}

    @Override
    public List<QualityIssue> check(MarketDataEntry entry, List<Schema> records, Listing listing, Clock clock) {
        List<QualityIssue> issues = new ArrayList<>();

        LocalDate entryDate = entry.getTimestamp().toLocalDate();
        int entryHour = entry.getTimestamp().getHour();
        String dateStr = entryDate.toString();

        List<HourlyListingStatistic> historicalRows = queryRecentHourRows(listing.listingId(), entryDate, entryHour);

        for (QualityStatistic statistic : statistics) {
            double value = statistic.compute(entry, records);
            if (Double.isNaN(value)) {
                continue;
            }

            List<HourlyListingStatistic> filteredRows =
                    filterByLookback(historicalRows, entryDate.minusDays(statistic.lookbackDays()));
            HourlyStatisticsAggregator.AggregatedStats baseline =
                    HourlyStatisticsAggregator.aggregate(filteredRows, statistic.name());
            Warmup warmup = computeWarmup(filteredRows, statistic.name());

            if (detectAnomalies) {
                boolean fresh = warmup.latestDate() != null
                        && !warmup.latestDate().isBefore(entryDate.minusDays(statistic.minimumDays()));

                if (warmup.distinctDays() >= statistic.minimumDays()
                        && fresh
                        && statistic.isAnomalous(value, baseline.mean(), baseline.stddev())) {
                    issues.add(buildIssue(
                            entry,
                            listing,
                            clock,
                            statistic,
                            value,
                            baseline.mean(),
                            baseline.stddev(),
                            records.size()));
                }
            }

            if (updateStatistics) {
                atomicUpdateMetric(listing.listingId(), entryHour, dateStr, statistic.name(), value);
            }
        }

        return issues;
    }

    private List<HourlyListingStatistic> filterByLookback(List<HourlyListingStatistic> rows, LocalDate statisticStart) {
        List<HourlyListingStatistic> filtered = new ArrayList<>();
        for (HourlyListingStatistic row : rows) {
            if (!LocalDate.parse(row.getDate()).isBefore(statisticStart)) {
                filtered.add(row);
            }
        }
        return filtered;
    }

    private Warmup computeWarmup(List<HourlyListingStatistic> filteredRows, String metric) {
        int distinctDays = 0;
        LocalDate latestDate = null;
        for (HourlyListingStatistic row : filteredRows) {
            if (metric.equals(row.getMetric())) {
                distinctDays++;
                LocalDate rowDate = LocalDate.parse(row.getDate());
                if (latestDate == null || rowDate.isAfter(latestDate)) {
                    latestDate = rowDate;
                }
            }
        }
        return new Warmup(distinctDays, latestDate);
    }

    private List<HourlyListingStatistic> queryRecentHourRows(int listingId, LocalDate entryDate, int hour) {
        LocalDate startDate = entryDate.minusDays(maxLookback);
        String skStart = String.format("%02d#%s#", hour, startDate.toString());
        String skEnd = String.format("%02d#%s#~", hour, entryDate.toString());

        QueryConditional queryConditional = QueryConditional.sortBetween(
                Key.builder().partitionValue(listingId).sortValue(skStart).build(),
                Key.builder().partitionValue(listingId).sortValue(skEnd).build());

        List<HourlyListingStatistic> rows = new ArrayList<>();
        for (HourlyListingStatistic row :
                statisticsTable.query(queryConditional).items()) {
            rows.add(row);
        }
        return rows;
    }

    private void atomicUpdateMetric(int listingId, int hour, String date, String metric, double value) {
        String sk = HourlyListingStatistic.buildSk(hour, date, metric);
        dynamoDbClient.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(Map.of(
                        "listingId", AttributeValue.fromN(String.valueOf(listingId)),
                        "sk", AttributeValue.fromS(sk)))
                .updateExpression("ADD #cnt :one, #s :val, #sos :sq")
                .expressionAttributeNames(Map.of("#cnt", "count", "#s", "sum", "#sos", "sumOfSquares"))
                .expressionAttributeValues(Map.of(
                        ":one", AttributeValue.fromN("1"),
                        ":val", AttributeValue.fromN(String.valueOf(value)),
                        ":sq", AttributeValue.fromN(String.valueOf(value * value))))
                .build());
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
