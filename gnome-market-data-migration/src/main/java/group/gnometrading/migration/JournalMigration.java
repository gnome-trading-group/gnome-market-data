package group.gnometrading.migration;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Rewrites every uploaded journal segment in place in schemas v2 (readers find a session's journal by its prefix,
 * so the keys can't move). Each original is first saved under the backup directory, and a segment is never
 * overwritten without one. Segments with nothing to convert are left alone, so re-runs and sessions already on the
 * new jars are skipped.
 */
final class JournalMigration {

    private static final Pattern SEGMENT = Pattern.compile("^[^/]+/segment-\\d+\\.zst$");

    private final S3Files s3;
    private final StreamResolver streams;
    private final String bucket;
    private final PrintStream out;
    private final ThreadLocal<JournalSegment> converters = ThreadLocal.withInitial(JournalSegment::new);
    private final AtomicInteger rewritten = new AtomicInteger();
    private final AtomicInteger current = new AtomicInteger();
    private final AtomicLong bookRecords = new AtomicLong();
    private final AtomicLong coreRecords = new AtomicLong();
    private final ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();

    JournalMigration(final S3Files s3, final StreamResolver streams, final String bucket, final PrintStream out) {
        this.s3 = s3;
        this.streams = streams;
        this.bucket = bucket;
        this.out = out;
    }

    /** @return whether every segment converted (and, unless a dry run, was backed up, written and read back) */
    boolean run(final boolean dryRun, final int limit, final int threads, final Path backup)
            throws InterruptedException {
        final List<String> all = this.s3.keys(this.bucket);
        final List<String> segments =
                all.stream().filter(key -> SEGMENT.matcher(key).matches()).toList();
        this.out.printf(
                "%s: %d segments, %d other objects%n", this.bucket, segments.size(), all.size() - segments.size());

        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (final String key : segments.subList(0, Math.min(limit, segments.size()))) {
            pool.submit(() -> migrate(key, dryRun, backup));
        }
        pool.shutdown();
        pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        this.out.printf(
                "%s %d segments (%d book records converted, %d core records migrated), %d already current,"
                        + " %d failed%n",
                dryRun ? "Would rewrite" : "Rewrote",
                this.rewritten.get(),
                this.bookRecords.get(),
                this.coreRecords.get(),
                this.current.get(),
                this.failures.size());
        this.failures.stream().limit(50).forEach(failure -> this.out.println("  FAILED " + failure));
        return this.failures.isEmpty();
    }

    private void migrate(final String key, final boolean dryRun, final Path backup) {
        try {
            final byte[] original = this.s3.get(this.bucket, key);
            final JournalSegment.Converted converted =
                    this.converters.get().convert(S3Files.decompress(original), this.streams::streamId);
            if (!converted.changed()) {
                this.current.incrementAndGet();
                return;
            }
            check(converted);
            if (!dryRun) {
                saveBackup(backup.resolve(key), original);
                this.s3.put(this.bucket, key, S3Files.compress(converted.data()));
                if (!Arrays.equals(S3Files.decompress(this.s3.get(this.bucket, key)), converted.data())) {
                    throw new IllegalStateException("reads back different from what was written");
                }
            }
            this.rewritten.incrementAndGet();
            this.bookRecords.addAndGet(converted.bookRecords());
            this.coreRecords.addAndGet(converted.coreRecords());
        } catch (RuntimeException e) {
            this.failures.add(key + ": " + e.getMessage());
        }
    }

    /** The converted segment keeps every record in sequence order and has nothing left to convert. */
    private void check(final JournalSegment.Converted converted) {
        final long[] sequences = converted.sequences();
        for (int i = 1; i < sequences.length; i++) {
            if (sequences[i] != sequences[i - 1] + 1) {
                throw new IllegalStateException("sequence " + sequences[i - 1] + " is followed by " + sequences[i]);
            }
        }
        final JournalSegment.Converted again = new JournalSegment().convert(converted.data(), this.streams::streamId);
        if (again.changed() || again.records() != converted.records()) {
            throw new IllegalStateException("the converted segment still has records to convert");
        }
    }

    private static void saveBackup(final Path path, final byte[] original) {
        try {
            if (Files.exists(path)) {
                if (!Arrays.equals(Files.readAllBytes(path), original)) {
                    throw new IllegalStateException("backup " + path + " differs from the segment in S3");
                }
                return;
            }
            Files.createDirectories(path.getParent());
            Files.write(path, original);
        } catch (IOException e) {
            throw new UncheckedIOException("Backing up to " + path, e);
        }
    }
}
