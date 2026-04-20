package group.gnometrading.quality.model;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.EnhancedType;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbConvertedBy;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * DynamoDB bean for per-listing rolling statistics used in anomaly detection.
 *
 * Table schema:
 * - PK: listingId (number)
 * - statistics: Map of metric name to inner map with "mean", "m2", "count" keys
 *   (Welford's online algorithm state)
 * - lastUpdated: LocalDateTime
 *
 * The statistics map grows automatically as new QualityStatistic implementations are added —
 * no DynamoDB schema change required.
 */
@DynamoDbBean
public final class ListingStatistics {

    public static final String MEAN_KEY = "mean";
    public static final String M2_KEY = "m2";
    public static final String COUNT_KEY = "count";

    private Integer listingId;
    private Map<String, Map<String, Double>> statistics;
    private LocalDateTime lastUpdated;

    public ListingStatistics() {}

    @DynamoDbPartitionKey
    @DynamoDbAttribute("listingId")
    public Integer getListingId() {
        return listingId;
    }

    public void setListingId(Integer listingId) {
        this.listingId = listingId;
    }

    @DynamoDbAttribute("statistics")
    public Map<String, Map<String, Double>> getStatistics() {
        return statistics;
    }

    public void setStatistics(Map<String, Map<String, Double>> statistics) {
        this.statistics = statistics;
    }

    @DynamoDbAttribute("lastUpdated")
    @DynamoDbConvertedBy(LocalDateTimeConverter.class)
    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(LocalDateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    /**
     * Updates the Welford running statistics for the named metric with a new observed value.
     * Creates the metric entry if it does not yet exist.
     */
    public void updateMetric(String metricName, double value) {
        if (statistics == null) {
            statistics = new HashMap<>();
        }
        Map<String, Double> entry = statistics.computeIfAbsent(metricName, k -> {
            Map<String, Double> metricMap = new HashMap<>();
            metricMap.put(MEAN_KEY, 0.0);
            metricMap.put(M2_KEY, 0.0);
            metricMap.put(COUNT_KEY, 0.0);
            return metricMap;
        });

        double count = entry.get(COUNT_KEY) + 1;
        double mean = entry.get(MEAN_KEY);
        double m2 = entry.get(M2_KEY);

        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean;
        m2 += delta * delta2;

        entry.put(COUNT_KEY, count);
        entry.put(MEAN_KEY, mean);
        entry.put(M2_KEY, m2);
    }

    /**
     * Returns the current mean for the named metric, or 0.0 if not yet tracked.
     */
    public double getMean(String metricName) {
        if (statistics == null || !statistics.containsKey(metricName)) {
            return 0.0;
        }
        return statistics.get(metricName).getOrDefault(MEAN_KEY, 0.0);
    }

    /**
     * Returns the current standard deviation for the named metric, or 0.0 if fewer than 2 samples.
     */
    public double getStdDev(String metricName) {
        if (statistics == null || !statistics.containsKey(metricName)) {
            return 0.0;
        }
        Map<String, Double> entry = statistics.get(metricName);
        double count = entry.getOrDefault(COUNT_KEY, 0.0);
        if (count < 2) {
            return 0.0;
        }
        return Math.sqrt(entry.getOrDefault(M2_KEY, 0.0) / count);
    }

    /**
     * Returns the current sample count for the named metric.
     */
    public int getSampleCount(String metricName) {
        if (statistics == null || !statistics.containsKey(metricName)) {
            return 0;
        }
        return (int) statistics.get(metricName).getOrDefault(COUNT_KEY, 0.0).doubleValue();
    }

    public static final class LocalDateTimeConverter implements AttributeConverter<LocalDateTime> {
        @Override
        public AttributeValue transformFrom(LocalDateTime input) {
            if (input == null) {
                return AttributeValue.builder().nul(true).build();
            }
            return AttributeValue.builder()
                    .n(String.valueOf(input.toEpochSecond(ZoneOffset.UTC)))
                    .build();
        }

        @Override
        public LocalDateTime transformTo(AttributeValue input) {
            if (input == null || input.nul() != null && input.nul()) {
                return null;
            }
            return LocalDateTime.ofEpochSecond(Long.parseLong(input.n()), 0, ZoneOffset.UTC);
        }

        @Override
        public EnhancedType<LocalDateTime> type() {
            return EnhancedType.of(LocalDateTime.class);
        }

        @Override
        public AttributeValueType attributeValueType() {
            return AttributeValueType.N;
        }
    }
}
