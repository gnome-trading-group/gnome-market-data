package group.gnometrading.bbo;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import group.gnometrading.Dependencies;
import group.gnometrading.SecurityMaster;
import group.gnometrading.data.MarketDataEntry;
import group.gnometrading.schemas.Bbo1sDecoder;
import group.gnometrading.schemas.Bbo1sSchema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.Statics;
import group.gnometrading.sm.Listing;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public final class BboTimelineLambdaHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final Logger logger = LogManager.getLogger(BboTimelineLambdaHandler.class);
    private static final int DEFAULT_MAX_POINTS = 5_000;
    private static final int MAX_MINUTES = 2 * 24 * 60; // 2 days

    private final S3Client s3Client;
    private final SecurityMaster securityMaster;
    private final String finalBucketName;

    public BboTimelineLambdaHandler() {
        this(
                Dependencies.getInstance().getS3Client(),
                Dependencies.getInstance().getSecurityMaster(),
                Dependencies.getInstance().getFinalBucketName());
    }

    public BboTimelineLambdaHandler(S3Client s3Client, SecurityMaster securityMaster, String finalBucketName) {
        this.s3Client = s3Client;
        this.securityMaster = securityMaster;
        this.finalBucketName = finalBucketName;
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        int listingId = requireInt(event, "listingId");
        long startTimestamp = requireLong(event, "startTimestamp");
        long endTimestamp = requireLong(event, "endTimestamp");
        int maxPoints = optionalInt(event, "maxPoints", DEFAULT_MAX_POINTS);

        if (endTimestamp <= startTimestamp) {
            throw new IllegalArgumentException("endTimestamp must be after startTimestamp");
        }

        Listing listing = securityMaster.getListing(listingId);
        if (listing == null) {
            throw new IllegalArgumentException("Listing not found for listingId=" + listingId);
        }

        int securityId = listing.security().securityId();
        int exchangeId = listing.exchange().exchangeId();

        LocalDateTime start =
                LocalDateTime.ofEpochSecond(startTimestamp, 0, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);
        LocalDateTime end =
                LocalDateTime.ofEpochSecond(endTimestamp, 0, ZoneOffset.UTC).truncatedTo(ChronoUnit.MINUTES);

        int totalMinutes = (int) ChronoUnit.MINUTES.between(start, end);
        if (totalMinutes > MAX_MINUTES) {
            throw new IllegalArgumentException(
                    "Requested range exceeds maximum of %d minutes (%d days). Got %d minutes."
                            .formatted(MAX_MINUTES, MAX_MINUTES / (24 * 60), totalMinutes));
        }

        List<Map<String, Object>> dataPoints = IntStream.rangeClosed(0, totalMinutes)
                .parallel()
                .mapToObj(offset -> loadMinute(securityId, exchangeId, start.plusMinutes(offset)))
                .flatMap(List::stream)
                .sorted(Comparator.comparingLong(p -> (long) p.get("timestamp")))
                .collect(Collectors.toList());

        List<Map<String, Object>> sampled = lttbDownsample(dataPoints, maxPoints);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("listingId", listingId);
        result.put("dataPoints", sampled);
        return result;
    }

    private List<Map<String, Object>> loadMinute(int securityId, int exchangeId, LocalDateTime timestamp) {
        MarketDataEntry entry = new MarketDataEntry(
                securityId, exchangeId, SchemaType.BBO_1S, timestamp, MarketDataEntry.EntryType.AGGREGATED);

        try {
            List<Schema> records = entry.loadFromS3(s3Client, finalBucketName);
            List<Map<String, Object>> points = new ArrayList<>();
            for (Schema schema : records) {
                Map<String, Object> point = toPoint((Bbo1sSchema) schema);
                if (point != null) {
                    points.add(point);
                }
            }
            return points;
        } catch (NoSuchKeyException e) {
            return List.of();
        } catch (Exception e) {
            logger.warn("Failed to load BBO_1S for {}@{}#{}: {}", securityId, exchangeId, timestamp, e.getMessage());
            return List.of();
        }
    }

    private static Map<String, Object> toPoint(Bbo1sSchema bbo) {
        long bid = bbo.decoder.bidPrice0();
        long ask = bbo.decoder.askPrice0();
        boolean hasBid = bid != Bbo1sDecoder.bidPrice0NullValue();
        boolean hasAsk = ask != Bbo1sDecoder.askPrice0NullValue();
        if (!hasBid && !hasAsk) {
            return null;
        }
        long tsNanos = bbo.decoder.timestampEvent();
        if (tsNanos == 0) {
            return null;
        }
        double bidPrice = hasBid ? (double) bid / Statics.PRICE_SCALING_FACTOR : 0.0;
        double askPrice = hasAsk ? (double) ask / Statics.PRICE_SCALING_FACTOR : 0.0;
        double midPrice = hasBid && hasAsk ? (bidPrice + askPrice) / 2.0 : hasBid ? bidPrice : askPrice;

        Map<String, Object> point = new LinkedHashMap<>();
        point.put("timestamp", TimeUnit.NANOSECONDS.toSeconds(tsNanos));
        point.put("bidPrice", bidPrice);
        point.put("askPrice", askPrice);
        point.put("midPrice", midPrice);
        return point;
    }

    /**
     * Largest-Triangle-Three-Buckets downsampling. Preserves visual shape of the series
     * while reducing to at most {@code threshold} points.
     */
    private static List<Map<String, Object>> lttbDownsample(List<Map<String, Object>> data, int threshold) {
        if (data.size() <= threshold) {
            return data;
        }

        List<Map<String, Object>> sampled = new ArrayList<>(threshold);
        sampled.add(data.get(0));

        double bucketSize = (double) (data.size() - 2) / (threshold - 2);
        int prevIdx = 0;

        for (int i = 0; i < threshold - 2; i++) {
            int nextBucketStart = (int) Math.floor((i + 1) * bucketSize) + 1;
            int nextBucketEnd = Math.min((int) Math.floor((i + 2) * bucketSize) + 1, data.size() - 1);

            double avgY = 0;
            for (int j = nextBucketStart; j < nextBucketEnd; j++) {
                avgY += midPrice(data.get(j));
            }
            double avgX = (nextBucketStart + nextBucketEnd - 1) / 2.0;
            avgY /= (nextBucketEnd - nextBucketStart);

            int currentBucketStart = (int) Math.floor(i * bucketSize) + 1;
            int currentBucketEnd = (int) Math.floor((i + 1) * bucketSize) + 1;

            double pointAX = prevIdx;
            double pointAY = midPrice(data.get(prevIdx));

            double maxArea = -1;
            int maxIdx = currentBucketStart;
            for (int j = currentBucketStart; j < currentBucketEnd; j++) {
                double area = Math.abs(
                                (pointAX - avgX) * (midPrice(data.get(j)) - pointAY) - (pointAX - j) * (avgY - pointAY))
                        / 2.0;
                if (area > maxArea) {
                    maxArea = area;
                    maxIdx = j;
                }
            }

            sampled.add(data.get(maxIdx));
            prevIdx = maxIdx;
        }

        sampled.add(data.get(data.size() - 1));
        return sampled;
    }

    private static double midPrice(Map<String, Object> point) {
        return ((Number) point.get("midPrice")).doubleValue();
    }

    private static int requireInt(Map<String, Object> event, String key) {
        Object value = event.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        if (value instanceof Number num) {
            return num.intValue();
        }
        throw new IllegalArgumentException("Field " + key + " must be an integer");
    }

    private static long requireLong(Map<String, Object> event, String key) {
        Object value = event.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required field: " + key);
        }
        if (value instanceof Number num) {
            return num.longValue();
        }
        throw new IllegalArgumentException("Field " + key + " must be a number");
    }

    private static int optionalInt(Map<String, Object> event, String key, int defaultValue) {
        Object value = event.get(key);
        if (value instanceof Number num) {
            return num.intValue();
        }
        return defaultValue;
    }
}
