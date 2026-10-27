package group.gnometrading.inventory;

import group.gnometrading.data.StreamEntry;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Aggregates the final bucket's inventory into coverage statistics, globally and per stream. Keys that aren't stream
 * record files (an older layout, anything else in the bucket) are counted and skipped, so one stray file never fails
 * the day's run.
 */
public final class CoverageAggregator {

    private static final Logger logger = LogManager.getLogger(CoverageAggregator.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter MINUTE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private final Map<String, DateAggregation> globalByDate = new HashMap<>();
    private final Map<String, RecordAggregation> globalByRecord = new HashMap<>();
    private final Map<Integer, StreamAggregation> streams = new HashMap<>();
    private final Map<Integer, Map<String, DateAggregation>> streamByDate = new HashMap<>();
    private int skippedKeys;

    /**
     * Aggregate a list of inventory records.
     */
    public void aggregate(List<InventoryRecord> records) {
        for (InventoryRecord record : records) {
            Optional<StreamEntry> parsed = StreamEntry.parse(record.key());
            if (parsed.isEmpty()) {
                skippedKeys++;
                continue;
            }
            StreamEntry entry = parsed.get();
            String date = entry.getTimestamp().format(DATE_FORMAT);
            String minute = entry.getTimestamp().format(MINUTE_FORMAT);
            String recordType = entry.getRecordType().identifier();

            globalByDate.computeIfAbsent(date, k -> new DateAggregation()).add(recordType, minute, record.size());
            globalByRecord
                    .computeIfAbsent(recordType, k -> new RecordAggregation())
                    .add(minute, record.size());
            streams.computeIfAbsent(entry.getStreamId(), StreamAggregation::new)
                    .add(recordType, date, minute, record.size());
            streamByDate
                    .computeIfAbsent(entry.getStreamId(), k -> new HashMap<>())
                    .computeIfAbsent(date, k -> new DateAggregation())
                    .add(recordType, minute, record.size());
        }
        if (skippedKeys > 0) {
            logger.warn("Skipped {} keys that are not stream record files", skippedKeys);
        }
    }

    /**
     * Build DynamoDB coverage records from aggregated data.
     */
    public List<CoverageRecord> buildCoverageRecords() {
        List<CoverageRecord> records = new ArrayList<>();
        records.add(buildGlobalRecord());
        records.addAll(buildStreamRecords());
        return records;
    }

    int skippedKeys() {
        return skippedKeys;
    }

    private CoverageRecord buildGlobalRecord() {
        Map<String, Object> summary = new HashMap<>();
        summary.put(
                "totalFiles",
                globalByDate.values().stream().mapToInt(d -> d.fileCount).sum());
        summary.put(
                "totalMinutes",
                globalByDate.values().stream()
                        .flatMap(d -> d.minutes.stream())
                        .distinct()
                        .count());
        summary.put(
                "totalSizeBytes",
                globalByDate.values().stream().mapToLong(d -> d.totalSize).sum());
        summary.put("streamCount", streams.size());

        Map<String, Object> streamsMap = new HashMap<>();
        for (StreamAggregation stream : streams.values()) {
            streamsMap.put(String.valueOf(stream.streamId), stream.toMap());
        }
        summary.put("streams", streamsMap);

        Map<String, Object> recordTypes = new HashMap<>();
        for (Map.Entry<String, RecordAggregation> entry : globalByRecord.entrySet()) {
            recordTypes.put(entry.getKey(), entry.getValue().toMap());
        }
        summary.put("recordTypes", recordTypes);

        if (!globalByDate.isEmpty()) {
            List<String> dates = new ArrayList<>(globalByDate.keySet());
            Collections.sort(dates);
            summary.put("earliestDate", dates.get(0));
            summary.put("latestDate", dates.get(dates.size() - 1));
        }
        return new CoverageRecord(CoverageKey.globalKey(), CoverageKey.summaryKey(), summary);
    }

    private List<CoverageRecord> buildStreamRecords() {
        List<CoverageRecord> records = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, DateAggregation>> streamEntry : streamByDate.entrySet()) {
            String pk = CoverageKey.streamKey(streamEntry.getKey());
            Map<String, Object> summary = streams.get(streamEntry.getKey()).toMap();
            summary.put("totalDays", streamEntry.getValue().size());
            records.add(new CoverageRecord(pk, CoverageKey.summaryKey(), summary));
            for (Map.Entry<String, DateAggregation> dateEntry :
                    streamEntry.getValue().entrySet()) {
                records.add(new CoverageRecord(
                        pk,
                        CoverageKey.dateKey(dateEntry.getKey()),
                        dateEntry.getValue().toMap()));
            }
        }
        return records;
    }

    private static class DateAggregation {
        Set<String> minutes = new HashSet<>();
        Map<String, RecordAggregation> recordTypes = new HashMap<>();
        long totalSize = 0;
        int fileCount = 0;

        void add(String recordType, String minute, long size) {
            minutes.add(minute);
            recordTypes
                    .computeIfAbsent(recordType, k -> new RecordAggregation())
                    .add(minute, size);
            totalSize += size;
            fileCount++;
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("hasCoverage", true);
            map.put("totalMinutes", minutes.size());
            Map<String, Object> recordTypesMap = new HashMap<>();
            for (Map.Entry<String, RecordAggregation> entry : recordTypes.entrySet()) {
                Map<String, Object> recordMap = new HashMap<>();
                recordMap.put("hasCoverage", true);
                recordMap.put("minutes", entry.getValue().minutes.size());
                recordMap.put("sizeBytes", entry.getValue().totalSize);
                recordTypesMap.put(entry.getKey(), recordMap);
            }
            map.put("recordTypes", recordTypesMap);
            return map;
        }
    }

    private static class RecordAggregation {
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

    private static class StreamAggregation {
        final int streamId;
        Set<String> minutes = new HashSet<>();
        Set<String> dates = new HashSet<>();
        Set<String> recordTypes = new HashSet<>();
        long totalSize = 0;
        int fileCount = 0;

        StreamAggregation(int streamId) {
            this.streamId = streamId;
        }

        void add(String recordType, String date, String minute, long size) {
            minutes.add(minute);
            dates.add(date);
            recordTypes.add(recordType);
            totalSize += size;
            fileCount++;
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("streamId", streamId);
            map.put("fileCount", fileCount);
            map.put("totalMinutes", minutes.size());
            map.put("totalSizeBytes", totalSize);
            List<String> sortedDates = new ArrayList<>(dates);
            Collections.sort(sortedDates);
            if (!sortedDates.isEmpty()) {
                map.put("earliestDate", sortedDates.get(0));
                map.put("latestDate", sortedDates.get(sortedDates.size() - 1));
            }
            List<String> sortedRecords = new ArrayList<>(recordTypes);
            Collections.sort(sortedRecords);
            map.put("recordTypes", sortedRecords);
            return map;
        }
    }
}
