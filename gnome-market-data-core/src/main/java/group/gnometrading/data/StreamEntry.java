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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.agrona.concurrent.UnsafeBuffer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * One minute of one stream's records of one type, as stored in S3:
 * {@code {streamId}/{family}/{record}/{YYYY}/{MM}/{DD}/{hh}/{mm}.zst}. Fields are zero-padded so keys sort by time. A
 * file is zstd over SBE messages, read by walking each message's header, so it may mix versions written either side of
 * a release.
 */
public final class StreamEntry {

    private static final String SEGMENT = "([a-z0-9-]+)";
    private static final String MINUTE = "(\\d{4})/(\\d{2})/(\\d{2})/(\\d{2})/(\\d{2})";
    private static final Pattern FILE_PATTERN =
            Pattern.compile("^(\\d+)/" + SEGMENT + "/" + SEGMENT + "/" + MINUTE + "\\.zst$");

    public static final ChronoUnit CYCLE_CHRONO_UNIT = ChronoUnit.MINUTES;

    private final int streamId;
    private final RecordType recordType;
    private final LocalDateTime timestamp;

    public StreamEntry(int streamId, RecordType recordType, LocalDateTime timestamp) {
        this.streamId = streamId;
        this.recordType = recordType;
        this.timestamp = timestamp;
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

    public String getKey() {
        return "%d/%s/%s/%s.zst"
                .formatted(streamId, recordType.family().identifier(), recordType.identifier(), timePath(timestamp));
    }

    /**
     * The key of the raw frames one connection received in one minute, kept apart from the record files. A connection
     * carries many streams, so it is keyed by the source, the shard and the connection's name within the shard; the
     * uuid keeps a restarted process from overwriting the minute its predecessor wrote.
     */
    public static String rawCaptureKey(
            String source, String shard, String connection, LocalDateTime timestamp, String uuid) {
        return "raw/%s/%s/%s/%s/%s.zst".formatted(source, shard, connection, timePath(timestamp), uuid);
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
        return "StreamEntry{streamId=" + streamId + ", recordType=" + recordType + ", timestamp=" + timestamp + "}";
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
        return streamId == that.streamId && recordType == that.recordType && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, recordType, timestamp);
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
        Matcher matcher = FILE_PATTERN.matcher(key);
        if (!matcher.matches()) {
            return Optional.empty();
        }
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
        return Optional.of(new StreamEntry(Integer.parseInt(matcher.group(1)), recordType, timestamp));
    }
}
