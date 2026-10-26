package group.gnometrading.migration;

import java.lang.reflect.Method;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** Legacy MBP-10 records as core schema v0 and v1 stored them, with every field set to a value of its own. */
final class LegacyRecords {

    static final int EXCHANGE_ID = 5;
    static final long SECURITY_ID = 97_983L;
    static final long V0_SIZE_NULL = group.gnometrading.schemas.v0.Mbp10Encoder.sizeNullValue();

    private LegacyRecords() {}

    /** Level i's prices are 1000 + i, sizes 2000 + i, counts 3000 + i; level 9's bid size is null. */
    static byte[] v0Mbp10(final long sequence) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final group.gnometrading.schemas.v0.Mbp10Encoder encoder = new group.gnometrading.schemas.v0.Mbp10Encoder()
                .wrapAndApplyHeader(buffer, 0, new group.gnometrading.schemas.v0.MessageHeaderEncoder());
        encoder.exchangeId(EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .timestampEvent(100 + sequence)
                .timestampSent(200 + sequence)
                .timestampRecv(300 + sequence)
                .price(400)
                .size(V0_SIZE_NULL)
                .action(group.gnometrading.schemas.v0.Action.Trade)
                .side(group.gnometrading.schemas.v0.Side.Ask)
                .sequence(sequence)
                .depth((short) 3);
        encoder.flags().clear().maybeBadBook(true).snapshot(true).lastMessage(true);
        setLevels(encoder);
        encoder.bidSize9(V0_SIZE_NULL);
        return bytes(buffer, 8 + encoder.encodedLength());
    }

    static byte[] v1Mbp10(final long sequence) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final group.gnometrading.schemas.v1.Mbp10Encoder encoder = new group.gnometrading.schemas.v1.Mbp10Encoder()
                .wrapAndApplyHeader(buffer, 0, new group.gnometrading.schemas.v1.MessageHeaderEncoder());
        encoder.exchangeId(EXCHANGE_ID)
                .securityId(SECURITY_ID)
                .timestampEvent(100 + sequence)
                .timestampSent(200 + sequence)
                .timestampRecv(300 + sequence)
                .price(400)
                .size(5_000_000_000L)
                .action(group.gnometrading.schemas.v1.Action.Add)
                .side(group.gnometrading.schemas.v1.Side.Bid)
                .sequence(sequence)
                .depth((short) 0);
        encoder.flags().clear().topOfBook(true).marketByPrice(true).badTimestampRecv(true);
        setLevels(encoder);
        return bytes(buffer, 8 + encoder.encodedLength());
    }

    static byte[] concat(final byte[]... parts) {
        final ExpandableArrayBuffer out = new ExpandableArrayBuffer();
        int length = 0;
        for (final byte[] part : parts) {
            out.putBytes(length, part);
            length += part.length;
        }
        final byte[] data = new byte[length];
        out.getBytes(0, data);
        return data;
    }

    private static void setLevels(final Object encoder) {
        for (int level = 0; level < 10; level++) {
            set(encoder, "bidPrice" + level, 1000L + level);
            set(encoder, "askPrice" + level, 1100L + level);
            set(encoder, "bidSize" + level, 2000L + level);
            set(encoder, "askSize" + level, 2100L + level);
            set(encoder, "bidCount" + level, 3000L + level);
            set(encoder, "askCount" + level, 3100L + level);
        }
    }

    private static void set(final Object encoder, final String field, final long value) {
        try {
            final Method setter = encoder.getClass().getMethod(field, long.class);
            setter.invoke(encoder, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(field, e);
        }
    }

    private static byte[] bytes(final MutableDirectBuffer buffer, final int length) {
        final byte[] data = new byte[length];
        buffer.getBytes(0, data);
        return data;
    }
}
