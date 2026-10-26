package group.gnometrading.migration;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.StreamRecord;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes every legacy merged file again under its stream's key, converted to the book family. Each write triggers
 * the pipeline as a new merged minute would: the transformer rebuilds the minute's derived files in the final bucket,
 * gap and quality re-check it. Legacy keys stay until {@link Cleanup}.
 *
 * <p>Re-runnable: a minute whose new key already holds the same converted bytes is skipped; one holding different
 * bytes is reported, never overwritten.
 */
final class MergedMigration {

    private final S3Files s3;
    private final StreamResolver streams;
    private final String bucket;
    private final PrintStream out;
    private final ThreadLocal<LegacyFile> converters = ThreadLocal.withInitial(LegacyFile::new);
    private final AtomicInteger migrated = new AtomicInteger();
    private final AtomicInteger alreadyMigrated = new AtomicInteger();
    private final AtomicLong records = new AtomicLong();
    private final ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();

    MergedMigration(final S3Files s3, final StreamResolver streams, final String bucket, final PrintStream out) {
        this.s3 = s3;
        this.streams = streams;
        this.bucket = bucket;
        this.out = out;
    }

    /** @return whether every legacy file was converted (and, unless a dry run, written and read back) */
    boolean run(final boolean dryRun, final int limit, final int threads) throws InterruptedException {
        final List<LegacyKeys.Aggregated> legacy = new ArrayList<>();
        int current = 0;
        final List<String> unknown = new ArrayList<>();
        for (final String key : this.s3.keys(this.bucket)) {
            final var parsed = LegacyKeys.aggregated(key);
            if (parsed.isPresent() && parsed.get().record().equals(RecordType.MBP_10.identifier())) {
                legacy.add(parsed.get());
            } else if (StreamEntry.parse(key).isPresent()) {
                current++;
            } else {
                unknown.add(key);
            }
        }
        this.out.printf(
                "%s: %d legacy MBP-10 files, %d stream-keyed, %d other%n",
                this.bucket, legacy.size(), current, unknown.size());
        unknown.stream().limit(20).forEach(key -> this.out.println("  other: " + key));
        if (!resolveAll(legacy)) {
            return false;
        }

        final List<LegacyKeys.Aggregated> batch = legacy.subList(0, Math.min(limit, legacy.size()));
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (final LegacyKeys.Aggregated file : batch) {
            pool.submit(() -> migrate(file, dryRun));
        }
        pool.shutdown();
        pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        this.out.printf(
                "%s %d files (%d records)%s, %d already migrated, %d failed%n",
                dryRun ? "Would write" : "Wrote",
                this.migrated.get(),
                this.records.get(),
                dryRun ? "" : " and read back",
                this.alreadyMigrated.get(),
                this.failures.size());
        this.failures.stream().limit(50).forEach(failure -> this.out.println("  FAILED " + failure));
        return this.failures.isEmpty() && unknown.isEmpty();
    }

    /** Resolves every legacy file's stream up front, so a missing listing stops the run before it writes. */
    private boolean resolveAll(final List<LegacyKeys.Aggregated> legacy) {
        final Map<String, Integer> counts = new TreeMap<>();
        final List<String> unresolved = new ArrayList<>();
        for (final LegacyKeys.Aggregated file : legacy) {
            final String id = file.exchangeId() + "/" + file.securityId();
            if (counts.merge(id, 1, Integer::sum) == 1) {
                try {
                    this.streams.streamId(file.exchangeId(), file.securityId());
                } catch (IllegalStateException e) {
                    unresolved.add(id + ": " + e.getMessage());
                }
            }
        }
        this.out.printf("%d exchange/security pairs, %d without a stream%n", counts.size(), unresolved.size());
        unresolved.forEach(line -> this.out.println("  UNRESOLVED " + line));
        return unresolved.isEmpty();
    }

    private void migrate(final LegacyKeys.Aggregated file, final boolean dryRun) {
        try {
            final int streamId = this.streams.streamId(file.exchangeId(), file.securityId());
            final byte[] legacy = S3Files.decompress(this.s3.get(this.bucket, file.key()));
            final LegacyFile.Converted converted =
                    this.converters.get().convert(legacy, file.exchangeId(), file.securityId(), streamId);
            check(converted, streamId);
            final String key = new StreamEntry(
                            streamId, RecordType.MBP_10, file.minute(), StreamEntry.EntryType.AGGREGATED)
                    .getKey();
            if (!dryRun) {
                final byte[] existing = this.s3.getIfExists(this.bucket, key);
                if (existing != null) {
                    if (!Arrays.equals(S3Files.decompress(existing), converted.data())) {
                        throw new IllegalStateException(key + " already holds different data");
                    }
                    this.alreadyMigrated.incrementAndGet();
                    return;
                }
                this.s3.put(this.bucket, key, S3Files.compress(converted.data()));
                if (!Arrays.equals(S3Files.decompress(this.s3.get(this.bucket, key)), converted.data())) {
                    throw new IllegalStateException(key + " reads back different from what was written");
                }
            }
            this.migrated.incrementAndGet();
            this.records.addAndGet(converted.records());
        } catch (RuntimeException e) {
            this.failures.add(file.key() + ": " + e.getMessage());
        }
    }

    /** The converted file decodes as the runtime reads it: the same number of records, all of the stream. */
    private static void check(final LegacyFile.Converted converted, final int streamId) {
        final List<StreamRecord> decoded = StreamEntry.decode(converted.data());
        if (decoded.size() != converted.records()) {
            throw new IllegalStateException(
                    "Converted " + converted.records() + " records but " + decoded.size() + " decode");
        }
        for (final StreamRecord record : decoded) {
            if (record.streamId() != streamId || record.recordType != RecordType.MBP_10) {
                throw new IllegalStateException(
                        "A converted record decodes as stream " + record.streamId() + " " + record.recordType);
            }
        }
    }
}
