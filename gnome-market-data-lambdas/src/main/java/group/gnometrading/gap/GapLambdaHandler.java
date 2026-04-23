package group.gnometrading.gap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.S3Utils;
import group.gnometrading.SecurityMaster;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.sm.Listing;
import group.gnometrading.transformer.JobId;
import group.gnometrading.transformer.TransformationJob;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Lambda handler that detects gaps in merged market data files.
 * Triggered by S3 events via SQS when new merged files are created.
 * Checks for missing previous minutes and records gaps in DynamoDB.
 */
public final class GapLambdaHandler implements RequestHandler<SQSEvent, Void> {

    private static final Logger logger = LogManager.getLogger(GapLambdaHandler.class);
    private static final int TWO_DAYS_IN_MINUTES = Math.toIntExact(TimeUnit.DAYS.toMinutes(2));

    private final ObjectMapper objectMapper;
    private final S3Client s3Client;
    private final SecurityMaster securityMaster;
    private final DynamoDbTable<TransformationJob> transformJobsTable;
    private final DynamoDbTable<Gap> gapsTable;
    private final String mergedBucketName;
    private final Clock clock;

    /**
     * No-argument constructor for Lambda runtime.
     * Uses the Dependencies singleton to obtain shared instances.
     */
    public GapLambdaHandler() {
        this(
                Dependencies.getInstance().getS3Client(),
                Dependencies.getInstance().getSecurityMaster(),
                Dependencies.getInstance().getObjectMapper(),
                Dependencies.getInstance().getTransformJobsTable(),
                Dependencies.getInstance().getGapsTable(),
                Dependencies.getInstance().getMergedBucketName(),
                Dependencies.getInstance().getClock());
    }

    /**
     * Constructor for unit testing.
     * Allows injection of mock dependencies.
     *
     * @param s3Client S3 client for accessing merged data files
     * @param securityMaster SecurityMaster for listing information
     * @param objectMapper Jackson ObjectMapper for JSON parsing
     * @param transformJobsTable DynamoDB table for transformation jobs
     * @param gapsTable DynamoDB table for gap records
     * @param mergedBucketName Name of the S3 bucket containing merged data
     * @param clock Clock for timestamps
     */
    GapLambdaHandler(
            S3Client s3Client,
            SecurityMaster securityMaster,
            ObjectMapper objectMapper,
            DynamoDbTable<TransformationJob> transformJobsTable,
            DynamoDbTable<Gap> gapsTable,
            String mergedBucketName,
            Clock clock) {
        this.s3Client = s3Client;
        this.securityMaster = securityMaster;
        this.objectMapper = objectMapper;
        this.transformJobsTable = transformJobsTable;
        this.gapsTable = gapsTable;
        this.mergedBucketName = mergedBucketName;
        this.clock = clock;
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        try {
            Set<MarketDataEntry> mergedEntries = S3Utils.extractKeysFromS3Event(event, objectMapper);
            logger.info("Found {} merged entries in S3 event", mergedEntries.size());

            for (MarketDataEntry entry : mergedEntries) {
                processEntry(entry);
            }
        } catch (Exception e) {
            logger.error("Error processing messages: {}", e.getMessage());
            throw new RuntimeException("Failed to process messages", e);
        }
        return null;
    }

    private void processEntry(MarketDataEntry entry) {
        logger.info("Processing entry: {}", entry);

        Listing listing = securityMaster.getListing(entry.getExchangeId(), entry.getSecurityId());
        if (listing == null) {
            logger.warn("Listing not found for entry: {}", entry);
            return;
        }

        LocalDateTime currentTimestamp = entry.getTimestamp();

        LocalDateTime previousMinute = currentTimestamp.minusMinutes(1);

        if (hasGap(listing, previousMinute)) {
            LocalDateTime mostRecentMinute = findMostRecentMinute(listing, previousMinute);

            if (mostRecentMinute == null) {
                logger.info("No recent data found within 2 days for listing {}, treating as first entry", listing);
                return;
            }

            List<Gap> gaps = createGapRecords(listing, mostRecentMinute, currentTimestamp);

            for (Gap gap : gaps) {
                storeGap(gap);
            }

            logger.info("Detected {} gap(s) for listing {} between {} and {}",
                    gaps.size(), listing.listingId(), mostRecentMinute, currentTimestamp);
        }
    }

    private boolean hasGap(Listing listing, LocalDateTime previousMinute) {
        if (transformationJobExists(listing, previousMinute)) {
            logger.debug("Previous minute exists in transformation jobs: {}", previousMinute);
            return false;
        }

        if (s3ObjectExists(listing, previousMinute)) {
            logger.debug("Previous minute exists in S3: {}", previousMinute);
            return false;
        }

        logger.info("Gap detected: previous minute {} not found", previousMinute);
        return true;
    }

    private boolean transformationJobExists(Listing listing, LocalDateTime timestamp) {
        JobId jobId = new JobId(listing.listingId(), listing.exchange().schemaType());
        Key key = Key.builder()
                .partitionValue(jobId.toString())
                .sortValue(timestamp.toEpochSecond(ZoneOffset.UTC))
                .build();

        TransformationJob job = transformJobsTable.getItem(key);
        return job != null;
    }

    private boolean s3ObjectExists(Listing listing, LocalDateTime timestamp) {
        MarketDataEntry entry = new MarketDataEntry(listing, timestamp, MarketDataEntry.EntryType.AGGREGATED);

        try {
            s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(mergedBucketName)
                    .key(entry.getKey())
                    .build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    private LocalDateTime findMostRecentMinute(Listing listing, LocalDateTime startingFrom) {
        LocalDateTime checkTimestamp = startingFrom;
        LocalDateTime twoDaysAgo = startingFrom.minusMinutes(TWO_DAYS_IN_MINUTES);

        while (checkTimestamp.isAfter(twoDaysAgo)) {
            if (transformationJobExists(listing, checkTimestamp)) {
                logger.debug("Found most recent minute in transformation jobs: {}", checkTimestamp);
                return checkTimestamp;
            }

            if (s3ObjectExists(listing, checkTimestamp)) {
                logger.debug("Found most recent minute in S3: {}", checkTimestamp);
                return checkTimestamp;
            }

            checkTimestamp = checkTimestamp.minusMinutes(1);
        }

        logger.warn("No data found within 2 days before {}", startingFrom);
        return null;
    }

    private List<Gap> createGapRecords(
            Listing listing, LocalDateTime mostRecentMinute, LocalDateTime currentTimestamp) {
        List<Gap> gaps = new ArrayList<>();
        LocalDateTime gapStart = mostRecentMinute.plusMinutes(1);

        while (gapStart.isBefore(currentTimestamp)) {
            Gap gap = new Gap();
            gap.setListingId(listing.listingId());
            gap.setTimestamp(gapStart);
            gap.setStatus(GapStatus.UNREVIEWED);
            gap.setReason(GapReason.UNKNOWN);
            gap.setExpected(false);
            gap.setNote("Gap detected by gap detector lambda");
            gap.setCreatedAt(LocalDateTime.now(clock));

            gaps.add(gap);
            gapStart = gapStart.plusMinutes(1);
        }

        return gaps;
    }

    private void storeGap(Gap gap) {
        try {
            gapsTable.putItem(gap);
            logger.info("Stored gap: listingId={}, timestamp={}", gap.getListingId(), gap.getTimestamp());
        } catch (Exception e) {
            logger.error("Error storing gap: {}", e.getMessage());
        }
    }
}
