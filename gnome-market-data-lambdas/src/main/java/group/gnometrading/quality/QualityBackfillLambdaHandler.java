package group.gnometrading.quality;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import group.gnometrading.Dependencies;
import group.gnometrading.SecurityMaster;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.HourlyListingStatistic;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.rules.QualityRule;
import group.gnometrading.quality.rules.QualityRuleFactory;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

public final class QualityBackfillLambdaHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final long TIMEOUT_SAFETY_MARGIN_MS = 30_000;

    public enum Mode {
        STATISTICS,
        ISSUES,
        ALL
    }

    private final S3Client s3Client;
    private final SecurityMaster securityMaster;
    private final DynamoDbTable<QualityIssue> qualityIssuesTable;
    private final DynamoDbTable<HourlyListingStatistic> hourlyStatisticsTable;
    private final DynamoDbClient dynamoDbClient;
    private final String statisticsTableName;
    private final String mergedBucketName;
    private final Clock clock;

    public QualityBackfillLambdaHandler() {
        this(
                Dependencies.getInstance().getS3Client(),
                Dependencies.getInstance().getSecurityMaster(),
                Dependencies.getInstance().getQualityIssuesTable(),
                Dependencies.getInstance().getHourlyListingStatisticsTable(),
                Dependencies.getInstance().getDynamoDbClient(),
                Dependencies.getInstance().getListingStatisticsTableName(),
                Dependencies.getInstance().getMergedBucketName(),
                Dependencies.getInstance().getClock());
    }

    public QualityBackfillLambdaHandler(
            S3Client s3Client,
            SecurityMaster securityMaster,
            DynamoDbTable<QualityIssue> qualityIssuesTable,
            DynamoDbTable<HourlyListingStatistic> hourlyStatisticsTable,
            DynamoDbClient dynamoDbClient,
            String statisticsTableName,
            String mergedBucketName,
            Clock clock) {
        this.s3Client = s3Client;
        this.securityMaster = securityMaster;
        this.qualityIssuesTable = qualityIssuesTable;
        this.hourlyStatisticsTable = hourlyStatisticsTable;
        this.dynamoDbClient = dynamoDbClient;
        this.statisticsTableName = statisticsTableName;
        this.mergedBucketName = mergedBucketName;
        this.clock = clock;
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        int exchangeId = requireInt(event, "exchangeId");
        int securityId = requireInt(event, "securityId");
        LocalDate date = requireDate(event, "date");
        Mode mode = parseMode(event);
        boolean resetStatistics = (boolean) event.getOrDefault("resetStatistics", false);

        Listing listing = securityMaster.getListing(exchangeId, securityId);
        if (listing == null) {
            throw new IllegalArgumentException(
                    "Listing not found for exchangeId=" + exchangeId + ", securityId=" + securityId);
        }

        List<QualityRule> rules = buildRules(mode);

        if (resetStatistics && mode != Mode.ISSUES) {
            resetDateStatistics(listing.listingId(), date, context);
        }

        List<MarketDataEntry> entries =
                MarketDataEntry.getKeysForListingByDay(s3Client, mergedBucketName, listing, date.atStartOfDay());
        context.getLogger().log("Processing " + entries.size() + " entries for " + date + " mode=" + mode);

        int entriesProcessed = 0;
        int issuesFound = 0;

        for (MarketDataEntry entry : entries) {
            if (context.getRemainingTimeInMillis() < TIMEOUT_SAFETY_MARGIN_MS) {
                context.getLogger().log("Approaching timeout, stopping");
                break;
            }

            int entryIssues = processEntry(entry, rules, listing, context);
            if (entryIssues >= 0) {
                issuesFound += entryIssues;
                entriesProcessed++;
            }
        }

        context.getLogger()
                .log(String.format(
                        "Backfill done: date=%s mode=%s processed=%d issues=%d",
                        date, mode, entriesProcessed, issuesFound));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("date", date.toString());
        result.put("mode", mode.name().toLowerCase());
        result.put("entriesProcessed", entriesProcessed);
        result.put("issuesFound", issuesFound);
        return result;
    }

    private int processEntry(MarketDataEntry entry, List<QualityRule> rules, Listing listing, Context context) {
        try {
            List<Schema> records = entry.loadFromS3(s3Client, mergedBucketName);
            int issuesFound = 0;
            for (QualityRule rule : rules) {
                List<QualityIssue> issues = rule.check(entry, records, listing, clock);
                for (QualityIssue issue : issues) {
                    issue.setRecordCount(records.size());
                    storeIssue(issue, context);
                    issuesFound++;
                }
            }
            return issuesFound;
        } catch (Exception e) {
            context.getLogger().log("Error processing entry " + entry + ": " + e.getMessage());
            return -1;
        }
    }

    private void resetDateStatistics(int listingId, LocalDate date, Context context) {
        String dateStr = date.toString();
        QueryConditional all = QueryConditional.keyEqualTo(
                Key.builder().partitionValue(listingId).build());
        for (HourlyListingStatistic row : hourlyStatisticsTable.query(all).items()) {
            if (dateStr.equals(row.getDate())) {
                hourlyStatisticsTable.deleteItem(Key.builder()
                        .partitionValue(row.getListingId())
                        .sortValue(row.getSk())
                        .build());
            }
        }
        context.getLogger().log("Reset statistics for listingId=" + listingId + " date=" + dateStr);
    }

    private List<QualityRule> buildRules(Mode mode) {
        return switch (mode) {
            case STATISTICS -> QualityRuleFactory.buildStatisticsOnly(
                    hourlyStatisticsTable, dynamoDbClient, statisticsTableName);
            case ISSUES -> QualityRuleFactory.buildIssueDetection(
                    hourlyStatisticsTable, dynamoDbClient, statisticsTableName);
            case ALL -> QualityRuleFactory.buildAll(hourlyStatisticsTable, dynamoDbClient, statisticsTableName);
        };
    }

    private void storeIssue(QualityIssue issue, Context context) {
        try {
            qualityIssuesTable.putItem(issue);
        } catch (Exception e) {
            context.getLogger().log("Error storing quality issue: " + e.getMessage());
        }
    }

    private static Mode parseMode(Map<String, Object> event) {
        Object raw = event.get("mode");
        if (raw == null) {
            return Mode.ALL;
        }
        try {
            return Mode.valueOf(raw.toString().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Field 'mode' must be one of: statistics, issues, all. Got: " + raw);
        }
    }

    private static int requireInt(Map<String, Object> event, String key) {
        Object value = event.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        if (value instanceof Integer intValue) {
            return intValue;
        }
        if (value instanceof Number numValue) {
            return numValue.intValue();
        }
        throw new IllegalArgumentException("Field " + key + " must be an integer, got: " + value.getClass());
    }

    private static LocalDate requireDate(Map<String, Object> event, String key) {
        Object value = event.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        try {
            return LocalDate.parse(value.toString());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Field " + key + " must be a date in YYYY-MM-DD format, got: " + value);
        }
    }
}
