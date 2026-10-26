package group.gnometrading.data;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.schemas.Family;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.SbeMessage;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.schemas.migration.SbeMigrator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.agrona.concurrent.UnsafeBuffer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * One minute of one stream's records of one type, as stored in S3:
 * {@code {streamId}/{family}/{record}/{YYYY}/{MM}/{DD}/{hh}/{mm}.zst}, or for a collector's raw copy (the merger's
 * input) the same followed by {@code /{uuid}.zst}. Fields are zero-padded so keys sort by time. A file is zstd over
 * SBE messages, read by walking each message's header, so it may mix versions written either side of a release.
 */
public final class StreamEntry {

    private static final String SEGMENT = "([a-z0-9-]+)";
    private static final String MINUTE = "(\\d{4})/(\\d{2})/(\\d{2})/(\\d{2})/(\\d{2})";
    private static final Pattern AGGREGATED_FILE_PATTERN =
            Pattern.compile("^(\\d+)/" + SEGMENT + "/" + SEGMENT + "/" + MINUTE + "\\.zst$");
    private static final Pattern RAW_FILE_PATTERN =
            Pattern.compile("^(\\d+)/" + SEGMENT + "/" + SEGMENT + "/" + MINUTE + "/([^/]+)\\.zst$");
    private static final int UUID_LENGTH = 8;
    private static final int UUID_GROUP = 9;

    public static final ChronoUnit CYCLE_CHRONO_UNIT = ChronoUnit.MINUTES;

    public enum EntryType {
        RAW,
        AGGREGATED
    }

    private final int streamId;
    private final RecordType recordType;
    private final LocalDateTime timestamp;
    private final EntryType entryType;
    private final String uuid;

    public StreamEntry(int streamId, RecordType recordType, LocalDateTime timestamp, EntryType entryType) {
        this(
                streamId,
                recordType,
                timestamp,
                entryType,
                entryType == EntryType.RAW ? UUID.randomUUID().toString().substring(0, UUID_LENGTH) : null);
    }

    public StreamEntry(int streamId, RecordType recordType, LocalDateTime timestamp, EntryType entryType, String uuid) {
        this.streamId = streamId;
        this.recordType = recordType;
        this.timestamp = timestamp;
        this.entryType = entryType;
        this.uuid = uuid;
    }

    /** The minute's records, each brought to its schema's current version. Empty if the file is. */
    public List<StreamRecord> loadFromS3(S3Client s3Client, String bucket) {
        InputStream stream =
                s3Client.getObject(request -> request.bucket(bucket).key(this.getKey()));
        try (ZstdInputStream zstdStream = new ZstdInputStream(stream);
                ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            zstdStream.transferTo(buffer);
            return decode(buffer.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Splits stored bytes into records by walking each message's header. */
    public static List<StreamRecord> decode(byte[] stored) {
        List<StreamRecord> records = new ArrayList<>();
        if (stored.length == 0) {
            return records;
        }
        byte[] data = SbeMigrator.migrateStream(stored);
        UnsafeBuffer buffer = new UnsafeBuffer(data);
        int offset = 0;
        while (offset < data.length) {
            int templateId = SbeMessage.templateId(buffer, offset);
            RecordType type = RecordType.forTemplateId(templateId);
            if (type == null) {
                throw new IllegalStateException(
                        "Template " + templateId + " at byte " + offset + " is not a stream record");
            }
            StreamRecord record = type.newInstance();
            int length = record.totalMessageSize();
            if (offset + length > data.length) {
                throw new IllegalStateException("Data ends inside a record at byte " + offset);
            }
            record.buffer.putBytes(0, buffer, offset, length);
            records.add(record);
            offset += length;
        }
        return records;
    }

    public void saveToS3(S3Client s3Client, String bucket, List<? extends StreamRecord> records) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (StreamRecord record : records) {
            byte[] bytes = new byte[record.totalMessageSize()];
            record.buffer.getBytes(0, bytes, 0, bytes.length);
            outputStream.write(bytes);
        }
        ByteArrayOutputStream compressedOutput = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdStream = new ZstdOutputStream(compressedOutput)) {
            zstdStream.write(outputStream.toByteArray());
        }
        saveToS3(s3Client, bucket, compressedOutput.toByteArray());
    }

    public void saveToS3(S3Client s3Client, String bucket, byte[] data) {
        String key = getKey();
        s3Client.putObject(request -> request.key(key).bucket(bucket), RequestBody.fromBytes(data));
    }

    public int getStreamId() {
        return streamId;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public String getUuid() {
        return uuid;
    }

    public String getKey() {
        String minute = minutePrefix(streamId, recordType, timestamp);
        return switch (entryType) {
            case AGGREGATED -> minute + ".zst";
            case RAW -> minute + "/" + uuid + ".zst";
        };
    }

    /** The key of a venue's raw frames captured for a stream in one minute; kept apart from the record files. */
    public static String rawCaptureKey(int streamId, LocalDateTime timestamp, String uuid) {
        return "%d/raw/%s/%s.zst".formatted(streamId, timePath(timestamp), uuid);
    }

    private static String minutePrefix(int streamId, RecordType recordType, LocalDateTime timestamp) {
        return "%d/%s/%s/%s"
                .formatted(streamId, recordType.family().identifier(), recordType.identifier(), timePath(timestamp));
    }

    private static String timePath(LocalDateTime timestamp) {
        return "%04d/%02d/%02d/%02d/%02d"
                .formatted(
                        timestamp.getYear(),
                        timestamp.getMonthValue(),
                        timestamp.getDayOfMonth(),
                        timestamp.getHour(),
                        timestamp.getMinute());
    }

    @Override
    public String toString() {
        return "StreamEntry{streamId=" + streamId + ", recordType=" + recordType + ", timestamp=" + timestamp
                + ", entryType=" + entryType + ", uuid='" + uuid + "'}";
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        StreamEntry that = (StreamEntry) other;
        return streamId == that.streamId
                && recordType == that.recordType
                && Objects.equals(timestamp, that.timestamp)
                && entryType == that.entryType
                && Objects.equals(this.uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, recordType, timestamp, entryType, uuid);
    }

    /** Every collector's raw copy of one stream-minute, for the merger to choose between. */
    public static List<StreamEntry> getRawKeys(
            S3Client s3Client, String bucket, int streamId, LocalDateTime timestamp, RecordType recordType) {
        return list(s3Client, bucket, minutePrefix(streamId, recordType, timestamp) + "/");
    }

    /** A stream's files of one record type on one day, or every day when {@code day} is null, in time order. */
    public static List<StreamEntry> getKeysForStreamByDay(
            S3Client s3Client, String bucket, int streamId, RecordType recordType, LocalDateTime day) {
        String prefix = "%d/%s/%s/".formatted(streamId, recordType.family().identifier(), recordType.identifier());
        if (day != null) {
            prefix += "%04d/%02d/%02d/".formatted(day.getYear(), day.getMonthValue(), day.getDayOfMonth());
        }
        return list(s3Client, bucket, prefix);
    }

    /** The record types of one family a bucket holds files of for a stream, from the folders under its prefix. */
    public static List<RecordType> getRecordTypes(S3Client s3Client, String bucket, int streamId, Family family) {
        String prefix = "%d/%s/".formatted(streamId, family.identifier());
        var response = s3Client.listObjectsV2Paginator(
                request -> request.bucket(bucket).prefix(prefix).delimiter("/"));
        List<RecordType> recordTypes = new ArrayList<>();
        response.commonPrefixes().forEach(common -> {
            String identifier =
                    common.prefix().substring(prefix.length(), common.prefix().length() - 1);
            for (RecordType type : RecordType.values()) {
                if (type.family() == family && type.identifier().equals(identifier)) {
                    recordTypes.add(type);
                }
            }
        });
        return recordTypes;
    }

    private static List<StreamEntry> list(S3Client s3Client, String bucket, String prefix) {
        var response = s3Client.listObjectsV2Paginator(
                request -> request.bucket(bucket).prefix(prefix));
        return response.contents().stream()
                .map(S3Object::key)
                .map(StreamEntry::parse)
                .flatMap(Optional::stream)
                .sorted(Comparator.comparing(StreamEntry::getTimestamp))
                .toList();
    }

    /** The entry a key names, or empty for any key that isn't a stream record file (old layouts, inventories). */
    public static Optional<StreamEntry> parse(String key) {
        Matcher aggregated = AGGREGATED_FILE_PATTERN.matcher(key);
        if (aggregated.matches()) {
            return entry(aggregated, EntryType.AGGREGATED, null);
        }
        Matcher raw = RAW_FILE_PATTERN.matcher(key);
        if (raw.matches()) {
            return entry(raw, EntryType.RAW, raw.group(UUID_GROUP));
        }
        return Optional.empty();
    }

    private static Optional<StreamEntry> entry(Matcher matcher, EntryType entryType, String uuid) {
        RecordType recordType;
        try {
            recordType = RecordType.forIdentifier(matcher.group(3));
        } catch (IllegalArgumentException unknownRecord) {
            return Optional.empty();
        }
        if (!recordType.family().identifier().equals(matcher.group(2))) {
            return Optional.empty();
        }
        LocalDateTime timestamp = LocalDateTime.of(
                Integer.parseInt(matcher.group(4)),
                Integer.parseInt(matcher.group(5)),
                Integer.parseInt(matcher.group(6)),
                Integer.parseInt(matcher.group(7)),
                Integer.parseInt(matcher.group(8)));
        return Optional.of(new StreamEntry(Integer.parseInt(matcher.group(1)), recordType, timestamp, entryType, uuid));
    }
}
