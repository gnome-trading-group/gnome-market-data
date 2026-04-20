package group.gnometrading.quality;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import group.gnometrading.Dependencies;
import group.gnometrading.SecurityMaster;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.ListingStatistics;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.rules.BadDataFlagsRule;
import group.gnometrading.quality.rules.QualityRule;
import group.gnometrading.quality.rules.SequenceMonotonicityRule;
import group.gnometrading.quality.rules.StatisticalQualityRule;
import group.gnometrading.quality.rules.TimestampAlignmentRule;
import group.gnometrading.quality.statistics.MidPriceStatistic;
import group.gnometrading.quality.statistics.SpreadStatistic;
import group.gnometrading.quality.statistics.TickCountStatistic;
import group.gnometrading.quality.statistics.TradeFrequencyStatistic;
import group.gnometrading.quality.statistics.TradeVolumeStatistic;
import group.gnometrading.quality.statistics.VolatilityStatistic;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.services.s3.S3Client;

public final class QualityBackfillLambdaHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final long TIMEOUT_SAFETY_MARGIN_MS = 30_000;

    private final S3Client s3Client;
    private final SecurityMaster securityMaster;
    private final DynamoDbTable<QualityIssue> qualityIssuesTable;
    private final DynamoDbTable<ListingStatistics> listingStatisticsTable;
    private final String mergedBucketName;
    private final Clock clock;

    public QualityBackfillLambdaHandler() {
        this(
                Dependencies.getInstance().getS3Client(),
                Dependencies.getInstance().getSecurityMaster(),
                Dependencies.getInstance().getQualityIssuesTable(),
                Dependencies.getInstance().getListingStatisticsTable(),
                Dependencies.getInstance().getMergedBucketName(),
                Dependencies.getInstance().getClock());
    }

    QualityBackfillLambdaHandler(
            S3Client s3Client,
            SecurityMaster securityMaster,
            DynamoDbTable<QualityIssue> qualityIssuesTable,
            DynamoDbTable<ListingStatistics> listingStatisticsTable,
            String mergedBucketName,
            Clock clock) {
        this.s3Client = s3Client;
        this.securityMaster = securityMaster;
        this.qualityIssuesTable = qualityIssuesTable;
        this.listingStatisticsTable = listingStatisticsTable;
        this.mergedBucketName = mergedBucketName;
        this.clock = clock;
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        int exchangeId = requireInt(event, "exchangeId");
        int securityId = requireInt(event, "securityId");
        LocalDate startDate = requireDate(event, "startDate");
        LocalDate endDate = requireDate(event, "endDate");
        boolean includeStatistical = (boolean) event.getOrDefault("includeStatistical", false);
        boolean resetStatistics = (boolean) event.getOrDefault("resetStatistics", false);

        Listing listing = securityMaster.getListing(exchangeId, securityId);
        if (listing == null) {
            throw new IllegalArgumentException(
                    "Listing not found for exchangeId=" + exchangeId + ", securityId=" + securityId);
        }

        List<QualityRule> rules = buildRules(includeStatistical);

        if (resetStatistics && includeStatistical) {
            listingStatisticsTable.deleteItem(
                    Key.builder().partitionValue(listing.listingId()).build());
            context.getLogger().log("Reset listing statistics for listingId=" + listing.listingId());
        }

        int entriesProcessed = 0;
        int issuesFound = 0;
        LocalDate lastDateProcessed = null;
        boolean complete;

        for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
            if (context.getRemainingTimeInMillis() < TIMEOUT_SAFETY_MARGIN_MS) {
                context.getLogger().log("Approaching timeout, stopping at " + date);
                break;
            }

            List<MarketDataEntry> entries =
                    MarketDataEntry.getKeysForListingByDay(s3Client, mergedBucketName, listing, date.atStartOfDay());
            context.getLogger().log("Processing " + entries.size() + " entries for " + date);

            for (MarketDataEntry entry : entries) {
                if (context.getRemainingTimeInMillis() < TIMEOUT_SAFETY_MARGIN_MS) {
                    context.getLogger().log("Approaching timeout mid-day, stopping");
                    break;
                }

                int entryIssues = processEntry(entry, rules, listing, context);
                if (entryIssues >= 0) {
                    issuesFound += entryIssues;
                    entriesProcessed++;
                }
            }

            lastDateProcessed = date;
        }
        complete = endDate.equals(lastDateProcessed);

        context.getLogger()
                .log(String.format(
                        "Backfill complete=%b: processed %d entries, found %d issues, lastDate=%s",
                        complete, entriesProcessed, issuesFound, lastDateProcessed));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("entriesProcessed", entriesProcessed);
        result.put("issuesFound", issuesFound);
        result.put("lastDateProcessed", lastDateProcessed != null ? lastDateProcessed.toString() : null);
        result.put("complete", complete);
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

    private List<QualityRule> buildRules(boolean includeStatistical) {
        List<QualityRule> rules = new ArrayList<>();
        rules.add(new TimestampAlignmentRule());
        rules.add(new SequenceMonotonicityRule());
        rules.add(new BadDataFlagsRule());
        if (includeStatistical) {
            rules.add(new StatisticalQualityRule(
                    listingStatisticsTable,
                    List.of(
                            new TickCountStatistic(),
                            new SpreadStatistic(),
                            new MidPriceStatistic(),
                            new TradeVolumeStatistic(),
                            new TradeFrequencyStatistic(),
                            new VolatilityStatistic())));
        }
        return rules;
    }

    private void storeIssue(QualityIssue issue, Context context) {
        try {
            qualityIssuesTable.putItem(issue);
        } catch (Exception e) {
            context.getLogger().log("Error storing quality issue: " + e.getMessage());
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
