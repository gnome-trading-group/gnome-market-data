package group.gnometrading.migration;

import java.nio.file.Path;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * TEMPORARY (Phase 3). Converts one stage's stored data from schema v0/v1 to the stream-keyed schemas v2.
 *
 * <pre>
 * AWS_PROFILE=dev java -jar gnome-market-data-migration.jar merged   --stage dev [--dry-run] [--limit N] [--threads N]
 * AWS_PROFILE=dev java -jar gnome-market-data-migration.jar journals --stage dev --backup DIR [--dry-run] [--limit N]
 * AWS_PROFILE=dev java -jar gnome-market-data-migration.jar cleanup  --stage dev [--dry-run]
 * </pre>
 *
 * Exits 1 if anything failed or was left unconverted.
 */
public final class Main {

    private static final int DEFAULT_THREADS = 8;

    private Main() {}

    public static void main(final String[] args) throws InterruptedException {
        if (args.length == 0) {
            usage();
        }
        final String command = args[0];
        MigrationStage stage = null;
        boolean dryRun = false;
        int limit = Integer.MAX_VALUE;
        int threads = DEFAULT_THREADS;
        Path backup = null;
        int next = 1;
        while (next < args.length) {
            final String flag = args[next];
            final String value = next + 1 < args.length ? args[next + 1] : null;
            next += switch (flag) {
                case "--stage" -> {
                    stage = MigrationStage.of(value);
                    yield 2;
                }
                case "--dry-run" -> {
                    dryRun = true;
                    yield 1;
                }
                case "--limit" -> {
                    limit = Integer.parseInt(value);
                    yield 2;
                }
                case "--threads" -> {
                    threads = Integer.parseInt(value);
                    yield 2;
                }
                case "--backup" -> {
                    backup = Path.of(value);
                    yield 2;
                }
                default -> {
                    usage();
                    yield 1;
                }};
        }
        if (stage == null) {
            usage();
        }
        final S3Files s3 = new S3Files(S3Client.create());
        final StreamResolver streams = new StreamResolver(stage.securityMaster());
        final boolean ok =
                switch (command) {
                    case "merged" -> new MergedMigration(s3, streams, stage.mergedBucket(), System.out)
                            .run(dryRun, limit, threads);
                    case "journals" -> {
                        if (backup == null && !dryRun) {
                            System.err.println("journals rewrites segments in place, so it needs --backup DIR");
                            System.exit(2);
                        }
                        yield new JournalMigration(s3, streams, stage.journalsBucket(), System.out)
                                .run(dryRun, limit, threads, backup);
                    }
                    case "cleanup" -> new Cleanup(s3, streams, stage, System.out).run(dryRun);
                    default -> {
                        usage();
                        yield false;
                    }
                };
        System.exit(ok ? 0 : 1);
    }

    private static void usage() {
        System.err.println("usage: merged|journals|cleanup --stage dev|prod [--dry-run] [--limit N] [--threads N]"
                + " [--backup DIR]");
        System.exit(2);
    }
}
