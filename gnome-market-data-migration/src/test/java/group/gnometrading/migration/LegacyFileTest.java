package group.gnometrading.migration;

import static group.gnometrading.migration.LegacyRecords.EXCHANGE_ID;
import static group.gnometrading.migration.LegacyRecords.SECURITY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.book.Action;
import group.gnometrading.schemas.book.Mbp10Decoder;
import group.gnometrading.schemas.book.Side;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class LegacyFileTest {

    private static final int STREAM_ID = 1_962_670;

    private static List<Mbp10Schema> converted(final byte[] legacy) {
        final LegacyFile.Converted converted = new LegacyFile().convert(legacy, EXCHANGE_ID, SECURITY_ID, STREAM_ID);
        return StreamEntry.decode(converted.data()).stream()
                .map(r -> (Mbp10Schema) r)
                .toList();
    }

    @Test
    void aV0RecordKeepsEveryFieldUnderItsStreamWithSizesWidenedAndFlagsRenumbered() {
        final Mbp10Decoder record = converted(LegacyRecords.v0Mbp10(7)).get(0).decoder;

        assertEquals(STREAM_ID, record.streamId());
        assertEquals(107, record.timestampEvent());
        assertEquals(207, record.timestampSent());
        assertEquals(307, record.timestampRecv());
        assertEquals(400, record.price());
        assertEquals(Mbp10Decoder.sizeNullValue(), record.size(), "v0's uint32 null size stays null");
        assertEquals(Action.Trade, record.action());
        assertEquals(Side.Ask, record.side());
        assertEquals(7, record.sequence());
        assertEquals(3, record.depth());
        assertTrue(record.flags().badData(), "maybeBadBook becomes badData");
        assertTrue(record.flags().snapshot());
        assertTrue(record.flags().lastMessage());
        assertFalse(record.flags().topOfBook());
        assertFalse(record.flags().marketByPrice());
        assertFalse(record.flags().badTimestampRecv());
        assertEquals(1000, record.bidPrice0());
        assertEquals(1109, record.askPrice9());
        assertEquals(2000, record.bidSize0());
        assertEquals(2109, record.askSize9());
        assertEquals(Mbp10Decoder.bidSize9NullValue(), record.bidSize9());
        assertEquals(3000, record.bidCount0());
        assertEquals(3109, record.askCount9());
    }

    @Test
    void aV1RecordKeepsItsInt64SizesAndTheOtherFlags() {
        final Mbp10Decoder record = converted(LegacyRecords.v1Mbp10(8)).get(0).decoder;

        assertEquals(STREAM_ID, record.streamId());
        assertEquals(5_000_000_000L, record.size());
        assertEquals(Action.Add, record.action());
        assertEquals(Side.Bid, record.side());
        assertFalse(record.flags().badData());
        assertTrue(record.flags().topOfBook());
        assertTrue(record.flags().marketByPrice());
        assertTrue(record.flags().badTimestampRecv());
        assertEquals(2009, record.bidSize9());
        assertEquals(1105, record.askPrice5());
    }

    @Test
    void aFileMixingBothVersionsConvertsEveryRecordInOrder() {
        final byte[] legacy =
                LegacyRecords.concat(LegacyRecords.v0Mbp10(1), LegacyRecords.v1Mbp10(2), LegacyRecords.v0Mbp10(3));

        final LegacyFile.Converted converted = new LegacyFile().convert(legacy, EXCHANGE_ID, SECURITY_ID, STREAM_ID);

        assertEquals(3, converted.records());
        assertEquals(
                List.of(1L, 2L, 3L),
                StreamEntry.decode(converted.data()).stream()
                        .map(r -> ((Mbp10Schema) r).decoder.sequence())
                        .toList());
    }

    @Test
    void aRecordOfAnotherListingIsRefused() {
        assertThrows(IllegalStateException.class, () -> new LegacyFile()
                .convert(LegacyRecords.v0Mbp10(1), EXCHANGE_ID + 1, SECURITY_ID, STREAM_ID));
    }

    @Test
    void aFileCutOffPartWayThroughARecordIsRefused() {
        final byte[] record = LegacyRecords.v0Mbp10(1);

        assertThrows(IllegalStateException.class, () -> new LegacyFile()
                .convert(Arrays.copyOf(record, record.length - 1), EXCHANGE_ID, SECURITY_ID, STREAM_ID));
    }

    @Test
    void anAlreadyConvertedFileIsNotLegacy() {
        final byte[] current = new LegacyFile()
                .convert(LegacyRecords.v0Mbp10(1), EXCHANGE_ID, SECURITY_ID, STREAM_ID)
                .data();

        assertThrows(IllegalStateException.class, () -> new LegacyFile()
                .convert(current, EXCHANGE_ID, SECURITY_ID, STREAM_ID));
    }
}
