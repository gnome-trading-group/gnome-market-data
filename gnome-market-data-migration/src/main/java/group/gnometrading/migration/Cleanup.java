package group.gnometrading.migration;

import group.gnometrading.data.StreamEntry;
import group.gnometrading.schemas.RecordType;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Deletes the legacy layout once the stream-keyed data is verified: final files (rebuilt from the migrated merged
 * files), raw collector files (decision 6: not kept), and merged files, but only those whose stream-keyed copy
 * exists. Run after the exit tests pass; S3 has no versioning, so this can't be undone.
 */
final class Cleanup {

    private final S3Files s3;
    private final StreamResolver streams;
    private final MigrationStage stage;
    private final PrintStream out;

    Cleanup(final S3Files s3, final StreamResolver streams, final MigrationStage stage, final PrintStream out) {
        this.s3 = s3;
        this.streams = streams;
        this.stage = stage;
        this.out = out;
    }

    /** @return whether every legacy merged file had its migrated copy (the others are kept and listed) */
    boolean run(final boolean dryRun) {
        final List<String> mergedKeys = this.s3.keys(this.stage.mergedBucket());
        final Set<String> present = new HashSet<>(mergedKeys);
        final List<String> merged = new ArrayList<>();
        final List<String> unmigrated = new ArrayList<>();
        for (final String key : mergedKeys) {
            LegacyKeys.aggregated(key).ifPresent(file -> {
                final String migrated = new StreamEntry(
                                this.streams.streamId(file.exchangeId(), file.securityId()),
                                RecordType.forIdentifier(file.record()),
                                file.minute(),
                                StreamEntry.EntryType.AGGREGATED)
                        .getKey();
                (present.contains(migrated) ? merged : unmigrated).add(key);
            });
        }
        final List<String> finals = this.s3.keys(this.stage.finalBucket()).stream()
                .filter(key -> LegacyKeys.aggregated(key).isPresent())
                .toList();
        final List<String> raw = this.s3.keys(this.stage.rawBucket()).stream()
                .filter(LegacyKeys::isRaw)
                .toList();

        this.out.printf(
                "%s legacy keys: %d merged (%d more not migrated, kept), %d final, %d raw%n",
                dryRun ? "Would delete" : "Deleting", merged.size(), unmigrated.size(), finals.size(), raw.size());
        unmigrated.stream().limit(20).forEach(key -> this.out.println("  NOT MIGRATED " + key));
        if (!dryRun) {
            this.s3.delete(this.stage.mergedBucket(), merged);
            this.s3.delete(this.stage.finalBucket(), finals);
            this.s3.delete(this.stage.rawBucket(), raw);
            this.out.println("Deleted.");
        }
        return unmigrated.isEmpty();
    }
}
