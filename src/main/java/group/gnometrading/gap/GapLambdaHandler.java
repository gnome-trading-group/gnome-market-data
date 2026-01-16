package group.gnometrading.gap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.Dependencies;
import group.gnometrading.MarketDataEntry;
import group.gnometrading.S3Utils;
import group.gnometrading.SecurityMaster;
import group.gnometrading.sm.Listing;
import group.gnometrading.transformer.JobId;
import group.gnometrading.transformer.TransformationJob;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Lambda handler that detects gaps in merged market data files.
 * Triggered by S3 events via SQS when new merged files are created.
 * Checks for missing previous minutes and records gaps in DynamoDB.
 */
public class GapLambdaHandler implements RequestHandler<SQSEvent, Void> {

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
            Dependencies.getInstance().getClock()
        );
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
        context.getLogger().log("Processing entry: " + entry);

        Listing listing = securityMaster.getListing(entry.getExchangeId(), entry.getSecurityId());
        if (listing == null) {
            context.getLogger().log("Listing not found for entry: " + entry);
            return;
        }

        LocalDateTime currentTimestamp = entry.getTimestamp();

        LocalDateTime previousMinute = currentTimestamp.minusMinutes(1);

        if (hasGap(listing, previousMinute, context)) {
            LocalDateTime mostRecentMinute = findMostRecentMinute(listing, previousMinute, context);

            if (mostRecentMinute == null) {
                context.getLogger().log("No recent data found within 2 days for listing " + listing +
                    ", treating as first entry");
                return;
            }

            List<Gap> gaps = createGapRecords(listing, mostRecentMinute, currentTimestamp, context);

            for (Gap gap : gaps) {
                storeGap(gap, context);
            }

            context.getLogger().log(String.format("Detected %d gap(s) for listing %d between %s and %s",
                gaps.size(), listing.listingId(), mostRecentMinute, currentTimestamp));
        }
    }

    private boolean hasGap(Listing listing, LocalDateTime previousMinute, Context context) {
        if (transformationJobExists(listing, previousMinute)) {
            context.getLogger().log("Previous minute exists in transformation jobs: " + previousMinute);
            return false;
        }

        if (s3ObjectExists(listing, previousMinute)) {
            context.getLogger().log("Previous minute exists in S3: " + previousMinute);
            return false;
        }

        context.getLogger().log("Gap detected: previous minute " + previousMinute + " not found");
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

    private LocalDateTime findMostRecentMinute(Listing listing, LocalDateTime startingFrom, Context context) {
        LocalDateTime checkTimestamp = startingFrom;
        LocalDateTime twoDaysAgo = startingFrom.minusMinutes(TWO_DAYS_IN_MINUTES);

        while (checkTimestamp.isAfter(twoDaysAgo)) {
            if (transformationJobExists(listing, checkTimestamp)) {
                context.getLogger().log("Found most recent minute in transformation jobs: " + checkTimestamp);
                return checkTimestamp;
            }

            if (s3ObjectExists(listing, checkTimestamp)) {
                context.getLogger().log("Found most recent minute in S3: " + checkTimestamp);
                return checkTimestamp;
            }

            checkTimestamp = checkTimestamp.minusMinutes(1);
        }

        context.getLogger().log("No data found within 2 days before " + startingFrom);
        return null;
    }

    private List<Gap> createGapRecords(Listing listing, LocalDateTime mostRecentMinute, LocalDateTime currentTimestamp, Context context) {
        List<Gap> gaps = new ArrayList<>();
        LocalDateTime gapStart = mostRecentMinute.plusMinutes(1);

        while (gapStart.isBefore(currentTimestamp)) {
            Gap gap = new Gap();
            gap.setListingId(listing.listingId());
            gap.setTimestamp(gapStart);
            gap.setGapReason(GapReason.UNKNOWN);
            gap.setReviewed(false);
            gap.setExpected(false);
            gap.setNote("Gap detected by gap detector lambda");
            gap.setCreatedAt(LocalDateTime.now(clock));

            gaps.add(gap);
            gapStart = gapStart.plusMinutes(1);
        }

        return gaps;
    }

    private void storeGap(Gap gap, Context context) {
        try {
            gapsTable.putItem(gap);
            context.getLogger().log(String.format("Stored gap: listingId=%d, timestamp=%s",
                    gap.getListingId(), gap.getTimestamp()));
        } catch (Exception e) {
            context.getLogger().log("Error storing gap: " + e.getMessage());
        }
    }
}

