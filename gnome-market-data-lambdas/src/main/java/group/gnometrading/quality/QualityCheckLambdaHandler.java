package group.gnometrading.quality;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.S3Utils;
import group.gnometrading.SecurityMaster;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.quality.model.HourlyListingStatistic;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.rules.QualityRule;
import group.gnometrading.quality.rules.QualityRuleFactory;
import group.gnometrading.schemas.Schema;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

public final class QualityCheckLambdaHandler implements RequestHandler<SQSEvent, Void> {

    private final ObjectMapper objectMapper;
    private final S3Client s3Client;
    private final SecurityMaster securityMaster;
    private final DynamoDbTable<QualityIssue> qualityIssuesTable;
    private final String mergedBucketName;
    private final Clock clock;
    private final List<QualityRule> rules;

    public QualityCheckLambdaHandler() {
        this(
                Dependencies.getInstance().getObjectMapper(),
                Dependencies.getInstance().getS3Client(),
                Dependencies.getInstance().getSecurityMaster(),
                Dependencies.getInstance().getQualityIssuesTable(),
                Dependencies.getInstance().getMergedBucketName(),
                Dependencies.getInstance().getClock(),
                Dependencies.getInstance().getHourlyListingStatisticsTable(),
                Dependencies.getInstance().getDynamoDbClient(),
                Dependencies.getInstance().getListingStatisticsTableName());
    }

    QualityCheckLambdaHandler(
            ObjectMapper objectMapper,
            S3Client s3Client,
            SecurityMaster securityMaster,
            DynamoDbTable<QualityIssue> qualityIssuesTable,
            String mergedBucketName,
            Clock clock,
            DynamoDbTable<HourlyListingStatistic> hourlyStatisticsTable,
            DynamoDbClient dynamoDbClient,
            String statisticsTableName) {
        this.objectMapper = objectMapper;
        this.s3Client = s3Client;
        this.securityMaster = securityMaster;
        this.qualityIssuesTable = qualityIssuesTable;
        this.mergedBucketName = mergedBucketName;
        this.clock = clock;
        this.rules = QualityRuleFactory.buildAll(hourlyStatisticsTable, dynamoDbClient, statisticsTableName);
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        try {
            Set<MarketDataEntry> mergedEntries = S3Utils.extractKeysFromS3Event(event, context, objectMapper);
            context.getLogger().log("Found " + mergedEntries.size() + " merged entries in S3 event");

            for (MarketDataEntry entry : mergedEntries) {
                processEntry(entry, context);
            }
        } catch (Exception e) {
            context.getLogger().log("Error processing messages: " + e.getMessage());
            throw new RuntimeException("Failed to process messages", e);
        }
        return null;
    }

    private void processEntry(MarketDataEntry entry, Context context) {
        context.getLogger().log("Running quality checks on entry: " + entry);

        Listing listing = securityMaster.getListing(entry.getExchangeId(), entry.getSecurityId());
        if (listing == null) {
            context.getLogger().log("Listing not found for entry: " + entry);
            return;
        }

        List<Schema> records = entry.loadFromS3(s3Client, mergedBucketName);

        int totalIssues = 0;
        for (QualityRule rule : rules) {
            List<QualityIssue> issues = rule.check(entry, records, listing, clock);
            for (QualityIssue issue : issues) {
                issue.setRecordCount(records.size());
                storeIssue(issue, context);
            }
            totalIssues += issues.size();
        }

        context.getLogger()
                .log(String.format(
                        "Quality check complete for %s: %d issue(s) found across %d records",
                        entry, totalIssues, records.size()));
    }

    private void storeIssue(QualityIssue issue, Context context) {
        try {
            qualityIssuesTable.putItem(issue);
            context.getLogger()
                    .log(String.format(
                            "Stored quality issue: listingId=%d, issueId=%s",
                            issue.getListingId(), issue.getIssueId()));
        } catch (Exception e) {
            context.getLogger().log("Error storing quality issue: " + e.getMessage());
        }
    }
}
