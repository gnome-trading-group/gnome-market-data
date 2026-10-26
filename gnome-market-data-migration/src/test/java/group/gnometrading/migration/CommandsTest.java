package group.gnometrading.migration;

import static group.gnometrading.migration.LegacyRecords.EXCHANGE_ID;
import static group.gnometrading.migration.LegacyRecords.SECURITY_ID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

class CommandsTest {

    private static final String BUCKET = "bucket";
    private static final int STREAM_ID = 42;
    private static final String LEGACY = "97983/5/2026/8/24/0/7/mbp-10.zst";
    private static final String MIGRATED = "42/book/mbp-10/2026/08/24/00/07.zst";

    private final Map<String, byte[]> objects = new HashMap<>();
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private S3Files s3;
    private StreamResolver streams;

    @BeforeEach
    void setUp() {
        this.s3 = mock(S3Files.class);
        this.streams = mock(StreamResolver.class);
        when(this.streams.streamId(EXCHANGE_ID, SECURITY_ID)).thenReturn(STREAM_ID);
        when(this.s3.get(eq(BUCKET), anyString())).thenAnswer(call -> this.objects.get(call.<String>getArgument(1)));
        when(this.s3.getIfExists(eq(BUCKET), anyString()))
                .thenAnswer(call -> this.objects.get(call.<String>getArgument(1)));
        org.mockito.Mockito.doAnswer(call -> this.objects.put(call.getArgument(1), call.getArgument(2)))
                .when(this.s3)
                .put(eq(BUCKET), anyString(), any());
    }

    private PrintStream out() {
        return new PrintStream(this.output);
    }

    private MergedMigration merged() {
        return new MergedMigration(this.s3, this.streams, BUCKET, out());
    }

    private void storeLegacyMerged() {
        this.objects.put(
                LEGACY, S3Files.compress(LegacyRecords.concat(LegacyRecords.v0Mbp10(1), LegacyRecords.v1Mbp10(2))));
        when(this.s3.keys(BUCKET)).thenAnswer(call -> List.copyOf(this.objects.keySet()));
    }

    @Test
    void aDryRunConvertsEveryLegacyFileButWritesNothing() throws Exception {
        storeLegacyMerged();

        assertTrue(merged().run(true, Integer.MAX_VALUE, 2));

        verify(this.s3, never()).put(anyString(), anyString(), any());
        assertTrue(this.output.toString().contains("Would write 1 files (2 records)"), this.output.toString());
    }

    @Test
    void aLegacyMergedFileIsWrittenUnderItsStreamAndTheOldKeyIsKept() throws Exception {
        storeLegacyMerged();

        assertTrue(merged().run(false, Integer.MAX_VALUE, 2));

        assertTrue(this.objects.containsKey(MIGRATED));
        assertTrue(this.objects.containsKey(LEGACY));
        assertEquals(
                2,
                group.gnometrading.data.StreamEntry.decode(S3Files.decompress(this.objects.get(MIGRATED)))
                        .size());
    }

    @Test
    void aRerunSkipsMinutesAlreadyMigrated() throws Exception {
        storeLegacyMerged();
        merged().run(false, Integer.MAX_VALUE, 1);
        final byte[] written = this.objects.get(MIGRATED);

        assertTrue(merged().run(false, Integer.MAX_VALUE, 1));

        assertArrayEquals(written, this.objects.get(MIGRATED));
        assertTrue(this.output.toString().contains("1 already migrated"), this.output.toString());
    }

    @Test
    void aListingWithoutAStreamStopsTheRunBeforeAnyWrite() throws Exception {
        storeLegacyMerged();
        when(this.streams.streamId(EXCHANGE_ID, SECURITY_ID)).thenThrow(new IllegalStateException("no listing"));

        assertFalse(merged().run(false, Integer.MAX_VALUE, 1));

        verify(this.s3, never()).put(anyString(), anyString(), any());
    }

    @Test
    void aJournalSegmentIsBackedUpBeforeItIsRewritten(@TempDir final Path backup) throws Exception {
        final byte[] legacySegment = S3Files.compress(segment(LegacyRecords.v0Mbp10(1)));
        this.objects.put("session-a/segment-00000.zst", legacySegment);
        this.objects.put("session-b/segment-00000.zst", S3Files.compress(segment(new byte[0])));
        when(this.s3.keys(BUCKET)).thenReturn(List.of("session-a/segment-00000.zst", "session-b/segment-00000.zst"));

        assertTrue(new JournalMigration(this.s3, this.streams, BUCKET, out()).run(false, Integer.MAX_VALUE, 1, backup));

        assertArrayEquals(legacySegment, Files.readAllBytes(backup.resolve("session-a/segment-00000.zst")));
        assertFalse(Files.exists(backup.resolve("session-b/segment-00000.zst")), "nothing to convert, no rewrite");
        final InOrder order = inOrder(this.s3);
        order.verify(this.s3).get(BUCKET, "session-a/segment-00000.zst");
        order.verify(this.s3).put(eq(BUCKET), eq("session-a/segment-00000.zst"), any());
        verify(this.s3, never()).put(eq(BUCKET), eq("session-b/segment-00000.zst"), any());
        assertTrue(this.output.toString().contains("Rewrote 1 segments (1 book records"), this.output.toString());
    }

    @Test
    void cleanupDeletesOnlyMigratedMergedFilesAndEveryLegacyFinalAndRawFile() {
        final MigrationStage stage = MigrationStage.DEV;
        final String unmigrated = "97983/5/2026/8/24/0/8/mbp-10.zst";
        when(this.s3.keys(stage.mergedBucket())).thenReturn(List.of(LEGACY, MIGRATED, unmigrated));
        when(this.s3.keys(stage.finalBucket()))
                .thenReturn(List.of("97983/5/2026/8/24/0/7/trades.zst", "42/book/trades/2026/08/24/00/07.zst"));
        when(this.s3.keys(stage.rawBucket()))
                .thenReturn(List.of("97983/5/2026/8/24/0/7/mbp-10/abcd.zst", "42/raw/2026/08/24/00/07/ef.zst"));

        assertFalse(new Cleanup(this.s3, this.streams, stage, out()).run(false), "one merged minute wasn't migrated");

        verify(this.s3).delete(stage.mergedBucket(), List.of(LEGACY));
        verify(this.s3).delete(stage.finalBucket(), List.of("97983/5/2026/8/24/0/7/trades.zst"));
        verify(this.s3).delete(stage.rawBucket(), List.of("97983/5/2026/8/24/0/7/mbp-10/abcd.zst"));
    }

    /** A journal segment of one record holding {@code payload}, or of a current Intent when it is empty. */
    private static byte[] segment(final byte[] payload) {
        final byte[] record;
        if (payload.length == 0) {
            final group.gnometrading.schemas.Intent intent = new group.gnometrading.schemas.Intent();
            record = new byte[intent.totalMessageSize()];
            intent.buffer.getBytes(0, record);
        } else {
            record = payload;
        }
        final ExpandableArrayBuffer out = new ExpandableArrayBuffer();
        out.putLong(0, 1, ByteOrder.LITTLE_ENDIAN);
        out.putShort(10, (short) (payload.length == 0 ? 14 : 2), ByteOrder.LITTLE_ENDIAN);
        out.putShort(12, (short) record.length, ByteOrder.LITTLE_ENDIAN);
        out.putBytes(JournalSegment.RECORD_HEADER, record);
        final byte[] data = new byte[JournalSegment.RECORD_HEADER + record.length];
        out.getBytes(0, data);
        return data;
    }
}
