package group.gnometrading.coverage;

import group.gnometrading.MarketDataEntry;

import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Aggregates inventory records into coverage statistics at multiple levels:
 * - Global (all securities and exchanges)
 * - Security (one security, all exchanges)
 * - Security+Exchange (specific pair)
 */
public class CoverageAggregator {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter MINUTE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    
    // Global aggregates
    private final Map<String, DateAggregation> globalByDate = new HashMap<>();
    private final Map<String, SchemaAggregation> globalBySchema = new HashMap<>();
    private final Map<String, SecurityExchangeAggregation> globalSecurityExchanges = new HashMap<>();
    
    // Security aggregates (securityId -> date -> aggregation)
    private final Map<Integer, Map<String, DateAggregation>> securityByDate = new HashMap<>();
    private final Map<Integer, Map<Integer, ExchangeAggregation>> securityExchanges = new HashMap<>();
    
    // Security+Exchange aggregates (key -> date -> aggregation)
    private final Map<String, Map<String, DateAggregation>> secExByDate = new HashMap<>();
    
    /**
     * Aggregate a list of inventory records.
     */
    public void aggregate(List<InventoryRecord> records) {
        for (InventoryRecord record : records) {
            MarketDataEntry entry = MarketDataEntry.fromKey(record.key());
            assert entry.getEntryType() == MarketDataEntry.EntryType.AGGREGATED : "Expected aggregated entry, got " + entry.getEntryType();

            String dateStr = entry.getTimestamp().format(DATE_FORMAT);
            String minuteKey = entry.getTimestamp().format(MINUTE_FORMAT);

            aggregateGlobal(entry, record.size(), dateStr, minuteKey);
            aggregateSecurity(entry, record.size(), dateStr, minuteKey);
            aggregateSecurityExchange(entry, record.size(), dateStr, minuteKey);
        }
    }
    
    /**
     * Build DynamoDB coverage records from aggregated data.
     */
    public List<CoverageRecord> buildCoverageRecords() {
        List<CoverageRecord> records = new ArrayList<>();
        
        records.addAll(buildGlobalRecords());
        records.addAll(buildSecurityRecords());
        records.addAll(buildSecurityExchangeRecords());
        
        return records;
    }
    
    private void aggregateGlobal(MarketDataEntry entry, long size, String date, String minute) {
        // Track by date
        globalByDate.computeIfAbsent(date, k -> new DateAggregation())
                .add(entry.getSchemaType().getIdentifier(), minute, size);
        
        // Track by schema type
        globalBySchema.computeIfAbsent(entry.getSchemaType().getIdentifier(), k -> new SchemaAggregation())
                .add(minute, size);
        
        // Track security-exchange pairs
        String secExKey = entry.getSecurityId() + "-" + entry.getExchangeId();
        globalSecurityExchanges.computeIfAbsent(secExKey, k ->
                new SecurityExchangeAggregation(entry.getSecurityId(), entry.getExchangeId()))
                .add(entry.getSchemaType().getIdentifier(), date, minute, size);
    }
    
    private void aggregateSecurity(MarketDataEntry entry, long size, String date, String minute) {
        // Track by date for this security
        securityByDate.computeIfAbsent(entry.getSecurityId(), k -> new HashMap<>())
                .computeIfAbsent(date, k -> new DateAggregation())
                .addWithExchange(entry.getExchangeId(), entry.getSchemaType().getIdentifier(), minute, size);
        
        // Track exchanges for this security
        securityExchanges.computeIfAbsent(entry.getSecurityId(), k -> new HashMap<>())
                .computeIfAbsent(entry.getExchangeId(), k -> new ExchangeAggregation(entry.getExchangeId()))
                .add(entry.getSchemaType().getIdentifier(), date, minute, size);
    }
    
    private void aggregateSecurityExchange(MarketDataEntry entry, long size, String date, String minute) {
        String key = entry.getSecurityId() + "#" + entry.getExchangeId();

        secExByDate.computeIfAbsent(key, k -> new HashMap<>())
                .computeIfAbsent(date, k -> new DateAggregation())
                .add(entry.getSchemaType().getIdentifier(), minute, size);
    }
    
    private List<CoverageRecord> buildGlobalRecords() {
        List<CoverageRecord> records = new ArrayList<>();
        
        // Build global summary
        Map<String, Object> globalSummary = new HashMap<>();
        globalSummary.put("totalFiles", globalByDate.values().stream().mapToInt(d -> d.fileCount).sum());
        globalSummary.put("totalMinutes", globalByDate.values().stream()
                .flatMap(d -> d.minutes.stream()).distinct().count());
        globalSummary.put("totalSizeBytes", globalByDate.values().stream().mapToLong(d -> d.totalSize).sum());
        globalSummary.put("securityExchangeCount", globalSecurityExchanges.size());

        // Add securities map
        Map<String, Object> securities = new HashMap<>();
        for (Map.Entry<String, SecurityExchangeAggregation> entry : globalSecurityExchanges.entrySet()) {
            securities.put(entry.getKey(), entry.getValue().toMap());
        }
        globalSummary.put("securities", securities);

        // Add schema types map
        Map<String, Object> schemaTypes = new HashMap<>();
        for (Map.Entry<String, SchemaAggregation> entry : globalBySchema.entrySet()) {
            schemaTypes.put(entry.getKey(), entry.getValue().toMap());
        }
        globalSummary.put("schemaTypes", schemaTypes);

        // Add date range
        if (!globalByDate.isEmpty()) {
            List<String> dates = new ArrayList<>(globalByDate.keySet());
            Collections.sort(dates);
            globalSummary.put("earliestDate", dates.get(0));
            globalSummary.put("latestDate", dates.get(dates.size() - 1));
        }

        records.add(new CoverageRecord(CoverageKey.globalKey(), CoverageKey.summaryKey(), globalSummary));

        return records;
    }

    private List<CoverageRecord> buildSecurityRecords() {
        List<CoverageRecord> records = new ArrayList<>();

        for (Map.Entry<Integer, Map<String, DateAggregation>> secEntry : securityByDate.entrySet()) {
            int securityId = secEntry.getKey();
            String pk = CoverageKey.securityKey(securityId);

            // Build summary for this security
            Map<String, Object> summary = new HashMap<>();
            summary.put("totalDays", secEntry.getValue().size());
            summary.put("totalMinutes", secEntry.getValue().values().stream()
                    .flatMap(d -> d.minutes.stream()).distinct().count());
            summary.put("totalSizeBytes", secEntry.getValue().values().stream()
                    .mapToLong(d -> d.totalSize).sum());

            // Add exchanges
            Map<Integer, ExchangeAggregation> exchanges = securityExchanges.get(securityId);
            if (exchanges != null) {
                Map<String, Object> exchangesMap = new HashMap<>();
                for (Map.Entry<Integer, ExchangeAggregation> exEntry : exchanges.entrySet()) {
                    exchangesMap.put(String.valueOf(exEntry.getKey()), exEntry.getValue().toMap());
                }
                summary.put("exchanges", exchangesMap);
                summary.put("totalExchanges", exchanges.size());
            }

            // Add date range
            List<String> dates = new ArrayList<>(secEntry.getValue().keySet());
            Collections.sort(dates);
            summary.put("earliestDate", dates.get(0));
            summary.put("latestDate", dates.get(dates.size() - 1));

            // Add all schema types
            Set<String> allSchemas = new HashSet<>();
            secEntry.getValue().values().forEach(d -> allSchemas.addAll(d.schemaTypes.keySet()));
            summary.put("schemaTypes", new ArrayList<>(allSchemas));

            records.add(new CoverageRecord(pk, CoverageKey.summaryKey(), summary));

            // Build date records
            for (Map.Entry<String, DateAggregation> dateEntry : secEntry.getValue().entrySet()) {
                records.add(new CoverageRecord(pk, CoverageKey.dateKey(dateEntry.getKey()),
                        dateEntry.getValue().toMapWithExchanges()));
            }
        }

        return records;
    }

    private List<CoverageRecord> buildSecurityExchangeRecords() {
        List<CoverageRecord> records = new ArrayList<>();

        for (Map.Entry<String, Map<String, DateAggregation>> entry : secExByDate.entrySet()) {
            String[] parts = entry.getKey().split("#");
            int securityId = Integer.parseInt(parts[0]);
            int exchangeId = Integer.parseInt(parts[1]);
            String pk = CoverageKey.securityExchangeKey(securityId, exchangeId);

            // Build summary
            Map<String, Object> summary = new HashMap<>();
            summary.put("totalDays", entry.getValue().size());
            summary.put("totalMinutes", entry.getValue().values().stream()
                    .flatMap(d -> d.minutes.stream()).distinct().count());
            summary.put("totalSizeBytes", entry.getValue().values().stream()
                    .mapToLong(d -> d.totalSize).sum());

            // Add date range
            List<String> dates = new ArrayList<>(entry.getValue().keySet());
            Collections.sort(dates);
            summary.put("earliestDate", dates.get(0));
            summary.put("latestDate", dates.get(dates.size() - 1));

            // Add all schema types
            Set<String> allSchemas = new HashSet<>();
            entry.getValue().values().forEach(d -> allSchemas.addAll(d.schemaTypes.keySet()));
            summary.put("schemaTypes", new ArrayList<>(allSchemas));

            records.add(new CoverageRecord(pk, CoverageKey.summaryKey(), summary));

            // Build date records
            for (Map.Entry<String, DateAggregation> dateEntry : entry.getValue().entrySet()) {
                records.add(new CoverageRecord(pk, CoverageKey.dateKey(dateEntry.getKey()),
                        dateEntry.getValue().toMap()));
            }
        }

        return records;
    }

    // Helper classes for aggregation

    private static class DateAggregation {
        Set<String> minutes = new HashSet<>();
        Map<String, SchemaAggregation> schemaTypes = new HashMap<>();
        Map<Integer, ExchangeDateAggregation> exchanges = new HashMap<>();
        long totalSize = 0;
        int fileCount = 0;

        void add(String schemaType, String minute, long size) {
            minutes.add(minute);
            schemaTypes.computeIfAbsent(schemaType, k -> new SchemaAggregation())
                    .add(minute, size);
            totalSize += size;
            fileCount++;
        }

        void addWithExchange(int exchangeId, String schemaType, String minute, long size) {
            add(schemaType, minute, size);
            exchanges.computeIfAbsent(exchangeId, k -> new ExchangeDateAggregation())
                    .add(schemaType, minute, size);
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("hasCoverage", true);
            map.put("totalMinutes", minutes.size());

            Map<String, Object> schemaTypesMap = new HashMap<>();
            for (Map.Entry<String, SchemaAggregation> entry : schemaTypes.entrySet()) {
                Map<String, Object> schemaMap = new HashMap<>();
                schemaMap.put("hasCoverage", true);
                schemaMap.put("minutes", entry.getValue().minutes.size());
                schemaMap.put("sizeBytes", entry.getValue().totalSize);
                schemaTypesMap.put(entry.getKey(), schemaMap);
            }
            map.put("schemaTypes", schemaTypesMap);

            return map;
        }

        Map<String, Object> toMapWithExchanges() {
            Map<String, Object> map = toMap();

            Map<String, Object> exchangesMap = new HashMap<>();
            for (Map.Entry<Integer, ExchangeDateAggregation> entry : exchanges.entrySet()) {
                exchangesMap.put(String.valueOf(entry.getKey()), entry.getValue().toMap());
            }
            map.put("exchanges", exchangesMap);

            return map;
        }
    }

    private static class SchemaAggregation {
        Set<String> minutes = new HashSet<>();
        long totalSize = 0;
        int fileCount = 0;

        void add(String minute, long size) {
            minutes.add(minute);
            totalSize += size;
            fileCount++;
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("fileCount", fileCount);
            map.put("minuteCount", minutes.size());
            map.put("sizeBytes", totalSize);
            return map;
        }
    }

    private static class SecurityExchangeAggregation {
        int securityId;
        int exchangeId;
        Set<String> minutes = new HashSet<>();
        Set<String> dates = new HashSet<>();
        Set<String> schemaTypes = new HashSet<>();
        long totalSize = 0;
        int fileCount = 0;

        SecurityExchangeAggregation(int securityId, int exchangeId) {
            this.securityId = securityId;
            this.exchangeId = exchangeId;
        }

        void add(String schemaType, String date, String minute, long size) {
            minutes.add(minute);
            dates.add(date);
            schemaTypes.add(schemaType);
            totalSize += size;
            fileCount++;
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("securityId", securityId);
            map.put("exchangeId", exchangeId);
            map.put("fileCount", fileCount);
            map.put("minuteCount", minutes.size());
            map.put("sizeBytes", totalSize);

            List<String> sortedDates = new ArrayList<>(dates);
            Collections.sort(sortedDates);
            if (!sortedDates.isEmpty()) {
                map.put("earliestDate", sortedDates.get(0));
                map.put("latestDate", sortedDates.get(sortedDates.size() - 1));
            }

            map.put("schemaTypes", new ArrayList<>(schemaTypes));
            return map;
        }
    }

    private static class ExchangeAggregation {
        int exchangeId;
        Set<String> minutes = new HashSet<>();
        Set<String> dates = new HashSet<>();
        Set<String> schemaTypes = new HashSet<>();
        long totalSize = 0;

        ExchangeAggregation(int exchangeId) {
            this.exchangeId = exchangeId;
        }

        void add(String schemaType, String date, String minute, long size) {
            minutes.add(minute);
            dates.add(date);
            schemaTypes.add(schemaType);
            totalSize += size;
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("exchangeId", exchangeId);
            map.put("totalDays", dates.size());
            map.put("totalMinutes", minutes.size());
            map.put("totalSizeBytes", totalSize);

            List<String> sortedDates = new ArrayList<>(dates);
            Collections.sort(sortedDates);
            if (!sortedDates.isEmpty()) {
                map.put("earliestDate", sortedDates.get(0));
                map.put("latestDate", sortedDates.get(sortedDates.size() - 1));
            }

            map.put("schemaTypes", new ArrayList<>(schemaTypes));
            return map;
        }
    }

    private static class ExchangeDateAggregation {
        Set<String> minutes = new HashSet<>();
        Map<String, SchemaAggregation> schemaTypes = new HashMap<>();

        void add(String schemaType, String minute, long size) {
            minutes.add(minute);
            schemaTypes.computeIfAbsent(schemaType, k -> new SchemaAggregation())
                    .add(minute, size);
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("minutes", minutes.size());

            Map<String, Object> schemaTypesMap = new HashMap<>();
            for (Map.Entry<String, SchemaAggregation> entry : schemaTypes.entrySet()) {
                Map<String, Object> schemaMap = new HashMap<>();
                schemaMap.put("minutes", entry.getValue().minutes.size());
                schemaMap.put("sizeBytes", entry.getValue().totalSize);
                schemaTypesMap.put(entry.getKey(), schemaMap);
            }
            map.put("schemaTypes", schemaTypesMap);

            return map;
        }
    }
}
