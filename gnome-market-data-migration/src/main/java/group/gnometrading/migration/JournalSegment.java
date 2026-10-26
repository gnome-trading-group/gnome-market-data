package group.gnometrading.migration;

import group.gnometrading.schemas.SbeMessage;
import group.gnometrading.schemas.migration.SbeMigrator;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.function.ToIntBiFunction;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Converts one journal segment (its uncompressed bytes) to schemas v2: legacy book records become book records of
 * their listing's stream (renumbered, so the record header's template id changes with them), older core records are
 * migrated to core v2, and everything else is copied as it is. Records keep their order, sequence and source.
 *
 * <p>Record layout (little-endian): globalSequence u64, sourceId u16, templateId u16, payloadLength u16, payload.
 * A zero sequence, or a record cut off by the end of the data, ends the segment (an open segment shipped as is).
 */
final class JournalSegment {

    static final int RECORD_HEADER = Long.BYTES + Short.BYTES + Short.BYTES + Short.BYTES;
    private static final int TEMPLATE_OFFSET = Long.BYTES + Short.BYTES;
    private static final int LENGTH_OFFSET = TEMPLATE_OFFSET + Short.BYTES;
    private static final int UINT16_MASK = 0xFFFF;

    private final LegacyMbp10 converter = new LegacyMbp10();
    private final ExpandableArrayBuffer out = new ExpandableArrayBuffer();
    private final ExpandableArrayBuffer payload = new ExpandableArrayBuffer();

    /** The converted segment and what changed in it. */
    record Converted(byte[] data, int records, int bookRecords, int coreRecords, long[] sequences) {
        boolean changed() {
            return this.bookRecords > 0 || this.coreRecords > 0;
        }
    }

    /**
     * @param streams the stream of each legacy record's exchange and security
     * @throws IllegalStateException if a record is a legacy book record other than MBP-10, or a core record no
     *     migration keeps
     */
    Converted convert(final byte[] segment, final ToIntBiFunction<Integer, Long> streams) {
        final UnsafeBuffer in = new UnsafeBuffer(segment);
        final long[] sequences = new long[segment.length / RECORD_HEADER];
        int offset = 0;
        int written = 0;
        int records = 0;
        int bookRecords = 0;
        int coreRecords = 0;
        while (segment.length - offset >= RECORD_HEADER) {
            final long sequence = in.getLong(offset, ByteOrder.LITTLE_ENDIAN);
            if (sequence == 0) {
                break;
            }
            final int sourceId = in.getShort(offset + Long.BYTES, ByteOrder.LITTLE_ENDIAN) & UINT16_MASK;
            int templateId = in.getShort(offset + TEMPLATE_OFFSET, ByteOrder.LITTLE_ENDIAN) & UINT16_MASK;
            final int length = in.getShort(offset + LENGTH_OFFSET, ByteOrder.LITTLE_ENDIAN) & UINT16_MASK;
            final int payloadOffset = offset + RECORD_HEADER;
            if (segment.length - payloadOffset < length) {
                break;
            }
            int payloadLength = length;
            UnsafeBuffer source = in;
            int sourceOffset = payloadOffset;
            if (length >= SbeMessage.HEADER_LENGTH && LegacyMbp10.isLegacyBook(in, payloadOffset)) {
                final int streamId = streams.applyAsInt(
                        this.converter.exchangeId(in, payloadOffset), this.converter.securityId(in, payloadOffset));
                payloadLength = this.converter.convert(in, payloadOffset, streamId, this.payload, 0);
                templateId = this.converter.convertedTemplateId();
                source = new UnsafeBuffer(this.payload.byteArray());
                sourceOffset = 0;
                bookRecords++;
            } else if (length >= SbeMessage.HEADER_LENGTH
                    && SbeMigrator.isGnomeSchema(SbeMessage.schemaId(in, payloadOffset))
                    && SbeMigrator.needsMigration(in, payloadOffset)) {
                payloadLength = SbeMigrator.migrateMessage(in, payloadOffset, this.payload, 0);
                if (payloadLength == 0) {
                    throw new IllegalStateException(
                            "Record " + sequence + " (template " + templateId + ") would be dropped by migration");
                }
                source = new UnsafeBuffer(this.payload.byteArray());
                sourceOffset = 0;
                coreRecords++;
            }
            this.out.putLong(written, sequence, ByteOrder.LITTLE_ENDIAN);
            this.out.putShort(written + Long.BYTES, (short) sourceId, ByteOrder.LITTLE_ENDIAN);
            this.out.putShort(written + TEMPLATE_OFFSET, (short) templateId, ByteOrder.LITTLE_ENDIAN);
            this.out.putShort(written + LENGTH_OFFSET, (short) payloadLength, ByteOrder.LITTLE_ENDIAN);
            this.out.putBytes(written + RECORD_HEADER, source, sourceOffset, payloadLength);
            written += RECORD_HEADER + payloadLength;
            offset = payloadOffset + length;
            sequences[records++] = sequence;
        }
        final byte[] data = new byte[written];
        this.out.getBytes(0, data);
        return new Converted(data, records, bookRecords, coreRecords, Arrays.copyOf(sequences, records));
    }
}
