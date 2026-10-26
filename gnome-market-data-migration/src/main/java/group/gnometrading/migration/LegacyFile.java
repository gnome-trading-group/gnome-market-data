package group.gnometrading.migration;

import group.gnometrading.schemas.SbeMessage;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** Converts one legacy merged file (its uncompressed bytes) of one exchange and security's MBP-10 to a stream's. */
final class LegacyFile {

    private final LegacyMbp10 converter = new LegacyMbp10();
    private final ExpandableArrayBuffer out = new ExpandableArrayBuffer();

    /** The converted records and how many there were. */
    record Converted(byte[] data, int records) {}

    /**
     * @throws IllegalStateException if the file holds anything but legacy MBP-10 of that exchange and security, or
     *     ends part way through a record
     */
    Converted convert(final byte[] legacy, final int exchangeId, final long securityId, final int streamId) {
        final UnsafeBuffer in = new UnsafeBuffer(legacy);
        int offset = 0;
        int written = 0;
        int records = 0;
        while (offset < legacy.length) {
            if (legacy.length - offset < SbeMessage.HEADER_LENGTH || !LegacyMbp10.isLegacyBook(in, offset)) {
                throw new IllegalStateException("Byte " + offset + " is not a legacy book record");
            }
            final int length = LegacyMbp10.length(in, offset);
            if (offset + length > legacy.length) {
                throw new IllegalStateException("File ends part way through the record at byte " + offset);
            }
            if (this.converter.exchangeId(in, offset) != exchangeId
                    || this.converter.securityId(in, offset) != securityId) {
                throw new IllegalStateException("Record at byte " + offset + " is exchange "
                        + this.converter.exchangeId(in, offset) + " security " + this.converter.securityId(in, offset)
                        + ", not the file's " + exchangeId + "/" + securityId);
            }
            written += this.converter.convert(in, offset, streamId, this.out, written);
            offset += length;
            records++;
        }
        final byte[] data = new byte[written];
        this.out.getBytes(0, data);
        return new Converted(data, records);
    }
}
