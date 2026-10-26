package group.gnometrading.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import group.gnometrading.schemas.Family;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.schemas.TradesSchema;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

@ExtendWith(MockitoExtension.class)
class StreamEntryTest {

    private static final LocalDateTime MINUTE = LocalDateTime.of(2026, 4, 5, 9, 7);

    @Mock
    private S3Client s3Client;

    @Mock
    private ListObjectsV2Iterable listing;

    @Test
    void keysAreStreamFamilyRecordThenZeroPaddedTime() {
        assertEquals(
                "1962670/book/mbp-10/2026/04/05/09/07.zst",
                new StreamEntry(1_962_670, RecordType.MBP_10, MINUTE, StreamEntry.EntryType.AGGREGATED).getKey());
        assertEquals(
                "42/book/trades/2026/04/05/09/07/abcd1234.zst",
                new StreamEntry(42, RecordType.TRADES, MINUTE, StreamEntry.EntryType.RAW, "abcd1234").getKey());
        assertEquals("42/raw/2026/04/05/09/07/f00d.zst", StreamEntry.rawCaptureKey(42, MINUTE, "f00d"));
    }

    @Test
    void keysParseBackToTheirEntry() {
        for (RecordType type : RecordType.values()) {
            StreamEntry aggregated = new StreamEntry(9, type, MINUTE, StreamEntry.EntryType.AGGREGATED);
            StreamEntry raw = new StreamEntry(9, type, MINUTE, StreamEntry.EntryType.RAW);
            assertEquals(Optional.of(aggregated), StreamEntry.parse(aggregated.getKey()));
            assertEquals(Optional.of(raw), StreamEntry.parse(raw.getKey()));
        }
    }

    @Test
    void keysOfOtherLayoutsParseToNothing() {
        assertTrue(StreamEntry.parse("532/151/2025/4/15/10/30/mbp-10.zst").isEmpty(), "the old layout");
        assertTrue(StreamEntry.parse("42/raw/2026/04/05/09/07/f00d.zst").isEmpty(), "raw capture");
        assertTrue(StreamEntry.parse("42/esports/mbp-10/2026/04/05/09/07.zst").isEmpty(), "record of another family");
        assertTrue(StreamEntry.parse("42/book/nope/2026/04/05/09/07.zst").isEmpty(), "unknown record");
        assertTrue(StreamEntry.parse("market-data-inventory/manifest.json").isEmpty());
    }

    @Test
    void rawEntriesGetAnIdOfTheirOwn() {
        assertNotNull(new StreamEntry(1, RecordType.MBP_10, MINUTE, StreamEntry.EntryType.RAW).getUuid());
        assertNull(new StreamEntry(1, RecordType.MBP_10, MINUTE, StreamEntry.EntryType.AGGREGATED).getUuid());
    }

    @Test
    void savedRecordsLoadBackInOrder() throws IOException {
        StreamEntry entry = new StreamEntry(7, RecordType.MBP_10, MINUTE, StreamEntry.EntryType.AGGREGATED);
        Mbp10Schema first = new Mbp10Schema();
        first.encoder.streamId(7).sequence(1).bidPrice0(100);
        Mbp10Schema second = new Mbp10Schema();
        second.encoder.streamId(7).sequence(2).bidPrice0(200);

        entry.saveToS3(s3Client, "bucket", List.of(first, second));
        ArgumentCaptor<RequestBody> body = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client).putObject(any(Consumer.class), body.capture());
        byte[] stored = body.getValue().contentStreamProvider().newStream().readAllBytes();
        serve(stored);

        List<StreamRecord> records = entry.loadFromS3(s3Client, "bucket");

        assertEquals(2, records.size());
        assertEquals(1, records.get(0).getSequenceNumber());
        assertEquals(200, ((Mbp10Schema) records.get(1)).decoder.bidPrice0());
        assertEquals(7, records.get(1).streamId());
        assertEquals(2 * first.totalMessageSize(), decompress(stored).length);
    }

    @Test
    void aFileMayMixRecordTypes() throws IOException {
        Mbp10Schema book = new Mbp10Schema();
        book.encoder.streamId(3).sequence(1);
        TradesSchema trade = new TradesSchema();
        trade.encoder.streamId(3).sequence(2);
        serve(compress(concat(bytes(book), bytes(trade))));

        List<StreamRecord> records = new StreamEntry(3, RecordType.MBP_10, MINUTE, StreamEntry.EntryType.AGGREGATED)
                .loadFromS3(s3Client, "b");

        assertEquals(RecordType.MBP_10, records.get(0).recordType);
        assertEquals(RecordType.TRADES, records.get(1).recordType);
    }

    @Test
    void aFileEndingInsideARecordIsRefused() throws IOException {
        Mbp10Schema book = new Mbp10Schema();
        byte[] whole = bytes(book);
        byte[] cut = new byte[whole.length - 3];
        System.arraycopy(whole, 0, cut, 0, cut.length);

        assertThrows(IllegalStateException.class, () -> StreamEntry.decode(cut));
    }

    @Test
    void anEmptyFileHasNoRecords() throws IOException {
        serve(compress(new byte[0]));

        assertTrue(new StreamEntry(1, RecordType.MBP_10, MINUTE, StreamEntry.EntryType.AGGREGATED)
                .loadFromS3(s3Client, "b")
                .isEmpty());
    }

    @Test
    void listsAStreamsDayInTimeOrderSkippingOtherKeys() {
        stubListing(
                "7/book/mbp-10/2026/04/05/09/08.zst",
                "7/book/mbp-10/2026/04/05/09/07.zst",
                "7/book/mbp-10/2026/04/05/notes.txt");

        List<StreamEntry> entries = StreamEntry.getKeysForStreamByDay(s3Client, "bucket", 7, RecordType.MBP_10, MINUTE);

        assertEquals(2, entries.size());
        assertEquals(7, entries.get(0).getTimestamp().getMinute());
        assertEquals(8, entries.get(1).getTimestamp().getMinute());
        ArgumentCaptor<Consumer<ListObjectsV2Request.Builder>> request = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).listObjectsV2Paginator(request.capture());
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder();
        request.getValue().accept(builder);
        assertEquals("7/book/mbp-10/2026/04/05/", builder.build().prefix());
    }

    @Test
    void rawKeysAreTheMinutesCollectorCopies() {
        stubListing("7/book/mbp-10/2026/04/05/09/07/aaaa.zst", "7/book/mbp-10/2026/04/05/09/07/bbbb.zst");

        List<StreamEntry> raws = StreamEntry.getRawKeys(s3Client, "raw", 7, MINUTE, RecordType.MBP_10);

        assertEquals(2, raws.size());
        assertEquals(StreamEntry.EntryType.RAW, raws.get(0).getEntryType());
    }

    private void stubListing(String... keys) {
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(listing);
        SdkIterable<S3Object> contents = () -> java.util.Arrays.stream(keys)
                .map(key -> S3Object.builder().key(key).build())
                .iterator();
        when(listing.contents()).thenReturn(contents);
    }

    private void serve(byte[] compressed) {
        when(s3Client.getObject(any(Consumer.class)))
                .thenAnswer(invocation -> new ResponseInputStream<>(
                        GetObjectResponse.builder().build(), new ByteArrayInputStream(compressed)));
    }

    private static byte[] bytes(StreamRecord record) {
        byte[] bytes = new byte[record.totalMessageSize()];
        record.buffer.getBytes(0, bytes, 0, bytes.length);
        return bytes;
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] out = new byte[first.length + second.length];
        System.arraycopy(first, 0, out, 0, first.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }

    private static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZstdOutputStream zstd = new ZstdOutputStream(out)) {
            zstd.write(data);
        }
        return out.toByteArray();
    }

    private static byte[] decompress(byte[] data) throws IOException {
        try (ZstdInputStream zstd = new ZstdInputStream(new ByteArrayInputStream(data))) {
            return zstd.readAllBytes();
        }
    }

    @Test
    void recordTypesAreTheFolderNamesUnderTheStreamsFamily() {
        SdkIterable<CommonPrefix> folders = () -> List.of(
                        CommonPrefix.builder().prefix("7/book/mbo/").build(),
                        CommonPrefix.builder().prefix("7/book/not-a-record/").build(),
                        CommonPrefix.builder().prefix("7/book/trades/").build())
                .iterator();
        when(listing.commonPrefixes()).thenReturn(folders);
        when(s3Client.listObjectsV2Paginator(any(Consumer.class))).thenReturn(listing);

        assertEquals(
                List.of(RecordType.MBO, RecordType.TRADES),
                StreamEntry.getRecordTypes(s3Client, "bucket", 7, Family.BOOK));

        ArgumentCaptor<Consumer<ListObjectsV2Request.Builder>> request = ArgumentCaptor.forClass(Consumer.class);
        verify(s3Client).listObjectsV2Paginator(request.capture());
        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder();
        request.getValue().accept(builder);
        assertEquals("7/book/", builder.build().prefix());
        assertEquals("/", builder.build().delimiter());
    }
}
