package group.gnometrading.migration;

import static group.gnometrading.migration.LegacyRecords.EXCHANGE_ID;
import static group.gnometrading.migration.LegacyRecords.SECURITY_ID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.schemas.Intent;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.OrderExecutionReport;
import group.gnometrading.schemas.SbeMessage;
import group.gnometrading.schemas.migration.SbeMigrator;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.ToIntBiFunction;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class JournalSegmentTest {

    private static final int STREAM_ID = 42;
    private static final int PADDING = 64;
    private static final ToIntBiFunction<Integer, Long> STREAMS = (exchangeId, securityId) -> {
        if (exchangeId != EXCHANGE_ID || securityId != SECURITY_ID) {
            throw new IllegalStateException("no stream for " + exchangeId + "/" + securityId);
        }
        return STREAM_ID;
    };

    private record Record(long sequence, int sourceId, int templateId, byte[] payload) {}

    private static byte[] segment(final Record... records) {
        final ExpandableArrayBuffer out = new ExpandableArrayBuffer();
        int length = 0;
        for (final Record record : records) {
            out.putLong(length, record.sequence(), ByteOrder.LITTLE_ENDIAN);
            out.putShort(length + 8, (short) record.sourceId(), ByteOrder.LITTLE_ENDIAN);
            out.putShort(length + 10, (short) record.templateId(), ByteOrder.LITTLE_ENDIAN);
            out.putShort(length + 12, (short) record.payload().length, ByteOrder.LITTLE_ENDIAN);
            out.putBytes(length + JournalSegment.RECORD_HEADER, record.payload());
            length += JournalSegment.RECORD_HEADER + record.payload().length;
        }
        final byte[] data = new byte[length + PADDING];
        out.getBytes(0, data, 0, length);
        return data;
    }

    private static List<Record> records(final byte[] segment) {
        final UnsafeBuffer in = new UnsafeBuffer(segment);
        final List<Record> records = new ArrayList<>();
        int offset = 0;
        while (offset < segment.length) {
            final int length = in.getShort(offset + 12, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
            records.add(new Record(
                    in.getLong(offset, ByteOrder.LITTLE_ENDIAN),
                    in.getShort(offset + 8, ByteOrder.LITTLE_ENDIAN) & 0xFFFF,
                    in.getShort(offset + 10, ByteOrder.LITTLE_ENDIAN) & 0xFFFF,
                    Arrays.copyOfRange(
                            segment,
                            offset + JournalSegment.RECORD_HEADER,
                            offset + JournalSegment.RECORD_HEADER + length)));
            offset += JournalSegment.RECORD_HEADER + length;
        }
        return records;
    }

    private static byte[] v0ExecutionReport() {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final group.gnometrading.schemas.v0.OrderExecutionReportEncoder encoder =
                new group.gnometrading.schemas.v0.OrderExecutionReportEncoder()
                        .wrapAndApplyHeader(buffer, 0, new group.gnometrading.schemas.v0.MessageHeaderEncoder());
        encoder.exchangeId(EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .orderId(99)
                .filledQty(77)
                .fillPrice(55);
        return Arrays.copyOf(buffer.byteArray(), 8 + encoder.encodedLength());
    }

    private static byte[] currentIntent() {
        final Intent intent = new Intent();
        intent.encoder
                .exchangeId(EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .bidPrice(10)
                .bidSize(1);
        final byte[] bytes = new byte[intent.totalMessageSize()];
        intent.buffer.getBytes(0, bytes);
        return bytes;
    }

    @Test
    void bookRecordsMoveToTheirStreamCoreRecordsMigrateAndCurrentOnesAreCopied() {
        final byte[] intent = currentIntent();
        final byte[] legacy = segment(
                new Record(10, 1, LegacyMbp10.LEGACY_MBP10_TEMPLATE_ID, LegacyRecords.v0Mbp10(10)),
                new Record(11, 2, 10, v0ExecutionReport()),
                new Record(12, 3, 14, intent));

        final JournalSegment.Converted converted = new JournalSegment().convert(legacy, STREAMS);

        assertTrue(converted.changed());
        assertEquals(3, converted.records());
        assertEquals(1, converted.bookRecords());
        assertEquals(1, converted.coreRecords());
        assertArrayEquals(new long[] {10, 11, 12}, converted.sequences());
        final List<Record> records = records(converted.data());
        assertEquals(List.of(1, 2, 3), records.stream().map(Record::sourceId).toList());
        assertEquals(
                List.of(new Mbp10Schema().templateId(), 10, 14),
                records.stream().map(Record::templateId).toList());

        final Mbp10Schema book =
                (Mbp10Schema) StreamEntry.decode(records.get(0).payload()).get(0);
        assertEquals(STREAM_ID, book.decoder.streamId());
        assertEquals(10, book.decoder.sequence());

        final UnsafeBuffer report = new UnsafeBuffer(records.get(1).payload());
        assertEquals(SbeMigrator.currentVersion(1), SbeMessage.version(report, 0));
        final OrderExecutionReport migrated = new OrderExecutionReport();
        migrated.buffer.putBytes(0, report, 0, records.get(1).payload().length);
        migrated.wrap(migrated.buffer);
        assertEquals(77, migrated.decoder.filledQty());
        assertEquals(55, migrated.decoder.fillPrice());

        assertArrayEquals(intent, records.get(2).payload());
    }

    @Test
    void aConvertedSegmentHasNothingLeftToConvert() {
        final byte[] converted = new JournalSegment()
                .convert(
                        segment(
                                new Record(1, 0, 2, LegacyRecords.v1Mbp10(1)),
                                new Record(2, 0, 10, v0ExecutionReport())),
                        STREAMS)
                .data();

        final JournalSegment.Converted again = new JournalSegment().convert(converted, STREAMS);

        assertFalse(again.changed());
        assertEquals(2, again.records());
        assertArrayEquals(converted, again.data());
    }

    @Test
    void aLegacyBookRecordOtherThanMbp10IsRefused() {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final group.gnometrading.schemas.v0.TradesEncoder trades = new group.gnometrading.schemas.v0.TradesEncoder()
                .wrapAndApplyHeader(buffer, 0, new group.gnometrading.schemas.v0.MessageHeaderEncoder());
        trades.exchangeId(EXCHANGE_ID).securityId(SECURITY_ID);
        final byte[] payload = Arrays.copyOf(buffer.byteArray(), 8 + trades.encodedLength());

        assertThrows(IllegalStateException.class, () -> new JournalSegment()
                .convert(segment(new Record(1, 0, 6, payload)), STREAMS));
    }

    @Test
    void aRecordCutOffByTheEndOfAnOpenSegmentEndsIt() {
        final byte[] whole =
                segment(new Record(1, 0, 14, currentIntent()), new Record(2, 0, 2, LegacyRecords.v0Mbp10(2)));
        final byte[] cut = Arrays.copyOf(whole, whole.length - PADDING - 5);

        final JournalSegment.Converted converted = new JournalSegment().convert(cut, STREAMS);

        assertEquals(1, converted.records());
        assertFalse(converted.changed());
    }
}
