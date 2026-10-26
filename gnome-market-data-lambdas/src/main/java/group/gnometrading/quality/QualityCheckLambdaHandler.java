package group.gnometrading.quality;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.SecurityMaster;
import group.gnometrading.SqsBatch;
import group.gnometrading.data.StreamEntry;
import group.gnometrading.quality.model.HourlyListingStatistic;
import group.gnometrading.quality.model.QualityIssue;
import group.gnometrading.quality.rules.QualityRule;
import group.gnometrading.quality.rules.QualityRuleFactory;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.sm.Listing;
import java.time.Clock;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

public final class QualityCheckLambdaHandler implements RequestHandler<SQSEvent, SQSBatchResponse> {

    private static final Logger logger = LogManager.getLogger(QualityCheckLambdaHandler.class);

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
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        return SqsBatch.forEachUnit(event, objectMapper, entry -> entry, false, this::processEntry);
    }

    private void processEntry(StreamEntry entry) {
        logger.info("Running quality checks on entry: {}", entry);

        Listing listing = securityMaster.listingForStream(entry.getStreamId());
        if (listing == null) {
            logger.warn("Listing not found for entry: {}", entry);
            return;
        }

        List<StreamRecord> records = entry.loadFromS3(s3Client, mergedBucketName);

        int totalIssues = 0;
        for (QualityRule rule : rules) {
            List<QualityIssue> issues = rule.check(entry, records, listing, clock);
            for (QualityIssue issue : issues) {
                issue.setRecordCount(records.size());
                storeIssue(issue);
            }
            totalIssues += issues.size();
        }

        logger.info(
                "Quality check complete for {}: {} issue(s) found across {} records",
                entry,
                totalIssues,
                records.size());
    }

    private void storeIssue(QualityIssue issue) {
        try {
            qualityIssuesTable.putItem(issue);
            logger.info("Stored quality issue: listingId={}, issueId={}", issue.getListingId(), issue.getIssueId());
        } catch (Exception e) {
            logger.error("Error storing quality issue: {}", e.getMessage());
        }
    }
}
