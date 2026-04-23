package group.gnometrading.quality;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import group.gnometrading.Dependencies;
import group.gnometrading.SecurityMaster;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.HourlyListingStatistic;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.rules.statistics.HourlyStatisticsAggregator;
import group.gnometrading.quality.rules.statistics.MidPriceStatistic;
import group.gnometrading.quality.rules.statistics.QualityStatistic;
import group.gnometrading.quality.rules.statistics.SpreadStatistic;
import group.gnometrading.quality.rules.statistics.TickCountStatistic;
import group.gnometrading.quality.rules.statistics.TradeFrequencyStatistic;
import group.gnometrading.quality.rules.statistics.TradeVolumeStatistic;
import group.gnometrading.quality.rules.statistics.VolatilityStatistic;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.sm.Listing;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public final class MinuteInvestigationLambdaHandler
        implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final Logger logger = LogManager.getLogger(MinuteInvestigationLambdaHandler.class);
    private static final int DEFAULT_WINDOW_MINUTES = 30;
    private static final int BASELINE_LOOKBACK_DAYS = 14;

    private static final List<QualityStatistic> STATISTICS = List.of(
            new TickCountStatistic(),
            new SpreadStatistic(),
            new MidPriceStatistic(),
            new TradeVolumeStatistic(),
            new TradeFrequencyStatistic(),
            new VolatilityStatistic());

    private final S3Client s3Client;
    private final SecurityMaster securityMaster;
    private final DynamoDbTable<QualityIssue> qualityIssuesTable;
    private final DynamoDbTable<HourlyListingStatistic> hourlyStatisticsTable;
    private final String mergedBucketName;

    public MinuteInvestigationLambdaHandler() {
        this(
                Dependencies.getInstance().getS3Client(),
                Dependencies.getInstance().getSecurityMaster(),
                Dependencies.getInstance().getQualityIssuesTable(),
                Dependencies.getInstance().getHourlyListingStatisticsTable(),
                Dependencies.getInstance().getMergedBucketName());
    }

    public MinuteInvestigationLambdaHandler(
            S3Client s3Client,
            SecurityMaster securityMaster,
            DynamoDbTable<QualityIssue> qualityIssuesTable,
            DynamoDbTable<HourlyListingStatistic> hourlyStatisticsTable,
            String mergedBucketName) {
        this.s3Client = s3Client;
        this.securityMaster = securityMaster;
        this.qualityIssuesTable = qualityIssuesTable;
        this.hourlyStatisticsTable = hourlyStatisticsTable;
        this.mergedBucketName = mergedBucketName;
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        int listingId = requireInt(event, "listingId");
        long centerTimestamp = requireLong(event, "timestamp");
        String schemaTypeStr = requireString(event, "schemaType");
        int windowMinutes = optionalInt(event, "windowMinutes", DEFAULT_WINDOW_MINUTES);

        Listing listing = securityMaster.getListing(listingId);
        if (listing == null) {
            throw new IllegalArgumentException("Listing not found for listingId=" + listingId);
        }

        SchemaType schemaType = SchemaType.findById(schemaTypeStr);

        LocalDateTime center = LocalDateTime.ofEpochSecond(centerTimestamp, 0, ZoneOffset.UTC);
        LocalDateTime windowStart = center.minusMinutes(windowMinutes);
        LocalDateTime windowEnd = center.plusMinutes(windowMinutes);

        List<Map<String, Object>> minutes = buildMinuteData(listing, schemaType, windowStart, windowEnd);
        List<Map<String, Object>> issues = fetchIssues(listingId, centerTimestamp, windowMinutes);
        Map<String, Object> baseline = computeBaseline(listingId, center);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("listingId", listingId);
        result.put("schemaType", schemaTypeStr);
        result.put("centerTimestamp", centerTimestamp);
        result.put("windowMinutes", windowMinutes);
        result.put("minutes", minutes);
        result.put("issues", issues);
        result.put("baseline", baseline);
        return result;
    }

    private List<Map<String, Object>> buildMinuteData(
            Listing listing, SchemaType schemaType, LocalDateTime windowStart, LocalDateTime windowEnd) {
        List<Map<String, Object>> result = new ArrayList<>();
        LocalDateTime current = windowStart;
        while (!current.isAfter(windowEnd)) {
            result.add(processMinute(listing, schemaType, current));
            current = current.plusMinutes(1);
        }
        return result;
    }

    private Map<String, Object> processMinute(Listing listing, SchemaType schemaType, LocalDateTime timestamp) {
        int securityId = listing.security().securityId();
        int exchangeId = listing.exchange().exchangeId();
        MarketDataEntry entry = new MarketDataEntry(
                securityId, exchangeId, schemaType, timestamp, MarketDataEntry.EntryType.AGGREGATED);

        Map<String, Object> minute = new LinkedHashMap<>();
        minute.put("timestamp", timestamp.toEpochSecond(ZoneOffset.UTC));

        try {
            List<Schema> records = entry.loadFromS3(s3Client, mergedBucketName);
            minute.put("hasData", true);
            minute.put("recordCount", records.size());

            Map<String, Object> metrics = new LinkedHashMap<>();
            for (QualityStatistic statistic : STATISTICS) {
                double value = statistic.compute(entry, records);
                if (!Double.isNaN(value)) {
                    metrics.put(statistic.name(), value);
                }
            }
            minute.put("metrics", metrics);
        } catch (NoSuchKeyException e) {
            minute.put("hasData", false);
            minute.put("recordCount", null);
            minute.put("metrics", Map.of());
        } catch (Exception e) {
            logger.warn("Failed to load data for minute {}: {}", timestamp, e.getMessage());
            minute.put("hasData", false);
            minute.put("recordCount", null);
            minute.put("metrics", Map.of());
        }

        return minute;
    }

    private List<Map<String, Object>> fetchIssues(int listingId, long centerTimestamp, int windowMinutes) {
        long startEpoch = centerTimestamp - (long) windowMinutes * 60;
        long endEpoch = centerTimestamp + (long) windowMinutes * 60;

        String skStart = String.valueOf(startEpoch);
        String skEnd = endEpoch + "#~";

        QueryConditional query = QueryConditional.sortBetween(
                Key.builder().partitionValue(listingId).sortValue(skStart).build(),
                Key.builder().partitionValue(listingId).sortValue(skEnd).build());

        List<Map<String, Object>> issues = new ArrayList<>();
        for (QualityIssue issue : qualityIssuesTable.query(query).items()) {
            Map<String, Object> issueMap = new LinkedHashMap<>();
            issueMap.put("issueId", issue.getIssueId());
            issueMap.put(
                    "ruleType",
                    issue.getRuleType() != null ? issue.getRuleType().name() : null);
            issueMap.put(
                    "timestamp",
                    issue.getTimestamp() != null ? issue.getTimestamp().toEpochSecond(ZoneOffset.UTC) : null);
            issueMap.put("details", issue.getDetails());
            issueMap.put("recordCount", issue.getRecordCount());
            issueMap.put("status", issue.getStatus() != null ? issue.getStatus().name() : null);
            issues.add(issueMap);
        }
        return issues;
    }

    private Map<String, Object> computeBaseline(int listingId, LocalDateTime center) {
        LocalDate centerDate = center.toLocalDate();
        int centerHour = center.getHour();

        LocalDate startDate = centerDate.minusDays(BASELINE_LOOKBACK_DAYS);
        String skStart = String.format("%02d#%s#", centerHour, startDate.toString());
        String skEnd = String.format("%02d#%s#~", centerHour, centerDate.toString());

        QueryConditional query = QueryConditional.sortBetween(
                Key.builder().partitionValue(listingId).sortValue(skStart).build(),
                Key.builder().partitionValue(listingId).sortValue(skEnd).build());

        List<HourlyListingStatistic> rows = new ArrayList<>();
        for (HourlyListingStatistic row : hourlyStatisticsTable.query(query).items()) {
            rows.add(row);
        }

        Map<String, Object> baseline = new LinkedHashMap<>();
        for (QualityStatistic statistic : STATISTICS) {
            HourlyStatisticsAggregator.AggregatedStats stats =
                    HourlyStatisticsAggregator.aggregate(rows, statistic.name());
            if (stats.count() > 0) {
                Map<String, Object> metricBaseline = new LinkedHashMap<>();
                metricBaseline.put("mean", stats.mean());
                metricBaseline.put("stddev", stats.stddev());
                metricBaseline.put("count", stats.sampleCount());
                baseline.put(statistic.name(), metricBaseline);
            }
        }
        return baseline;
    }

    private static int requireInt(Map<String, Object> event, String key) {
        Object value = event.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        if (value instanceof Number num) {
            return num.intValue();
        }
        throw new IllegalArgumentException("Field " + key + " must be an integer, got: " + value.getClass());
    }

    private static long requireLong(Map<String, Object> event, String key) {
        Object value = event.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        if (value instanceof Number num) {
            return num.longValue();
        }
        throw new IllegalArgumentException("Field " + key + " must be a number, got: " + value.getClass());
    }

    private static String requireString(Map<String, Object> event, String key) {
        Object value = event.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        return value.toString();
    }

    private static int optionalInt(Map<String, Object> event, String key, int defaultValue) {
        Object value = event.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number num) {
            return num.intValue();
        }
        return defaultValue;
    }
}
