# CLAUDE.md

Historical market data pipeline. Collectors (ECS Fargate, three regions) capture venue feeds as one-minute raw files, and SQS-driven Java Lambdas merge them, transform them into derived schemas, detect gaps, run quality checks and build coverage. The final bucket `gnome-market-data-<stage>` feeds backtests and research (gnomepy, gnome-backtest), and the `market-data-api` REST API backs the controller's Market Data pages (Collectors, Gaps, Transform Jobs, Coverage, Quality). A failure here delays or holes historical data; live trading does not depend on it.

## Commands

- Build all: `mvn clean package` (Java 17). Spotless and checkstyle run in `validate`, so a format slip fails the build.
- Fix formatting: `mvn spotless:apply`
- All tests: `mvn test`
- One test: `mvn test -pl gnome-market-data-lambdas -am -Dtest=group.gnometrading.merger.MergerLambdaHandlerTest -Dsurefire.failIfNoSpecifiedTests=false`. Use the full class name. The flag keeps `-am` from failing on core, which has no matching test.
- Lambdas only: `mvn clean package -pl gnome-market-data-lambdas -am`
- CDK: `cd cdk && npm ci && npm run build`. `npx cdk synth` needs Docker and `MAVEN_CREDENTIALS` (JSON with `GITHUB_ACTOR`/`GITHUB_TOKEN`), because every Java Lambda image builds the jar from source. There are no CDK tests.
- Maven, not Gradle. Do not run `cdk deploy`; the pipeline deploys.

## Layout

- `gnome-market-data-core`: `StreamEntry` (S3 key layout, zstd/SBE encode and decode, listing helpers). Published; gnome-backtest depends on it.
- `gnome-market-data-lambdas`: all Java Lambdas, built into one image per function (the handler class is chosen by `cmd`).
  - `merger/` Merger · `transformer/` JobCreator + JobProcessor · `gap/` Gap Detector · `coverage/` Inventory Processor
  - `quality/` QualityCheck, QualityBackfill, MinuteInvestigation, plus `rules/` · `bbo/` BboTimeline
  - `SqsBatch`, `S3Utils`: shared SQS and S3-notification handling. `Dependencies`: env-var wiring and the registry `SecurityMaster`.
- `cdk/lib/market-data-pipeline-stack.ts`: `MarketDataPipeline` and the per-stage `AppStage`. `COLLECTOR_REGIONS` = us-east-1, ap-northeast-1, eu-west-1.
- `cdk/lib/stacks/`:
  - `storage-stack`: buckets, DynamoDB tables, queues + DLQs, S3/SNS wiring
  - `event-bus-stack`: the `collector-ecs-events` bus
  - `collector-regional-stack`: VPC, cluster, base task definition and log group, one per region
  - `backend-stack`: API Gateway + Python Lambdas, Cognito auth, ECS task monitor
  - `merger-stack`, `transformer-stack`, `gap-detector-stack`, `inventory-processor-stack`, `quality-check-stack`, `monitoring-stack`
- `cdk/lib/constructs/java-lambda.ts`: shared Java Lambda (2048 MB default, 15 min timeout, JSON logs, one-month retention).
- `cdk/lambda/`: Python API handlers (`functions/{collectors,gaps,transform-jobs,coverage,quality-issues,listing-statistics,bbo}`) and the `common` layer.
- `cdk/lib/stacks/collector-start.sh`: the collector container entrypoint. `collector-docker/` and `java-lambda-docker/` are written at synth time, so edit the `.ts`/`.sh` sources, not those files.

## Conventions and invariants

- Record types come from gnome-schemas `RecordType`, all in family `book`: `mbo`, `mbp-10`, `mbp-1`, `bbo-1s`, `bbo-1m`, `trades`, `ohlcv-1s`, `ohlcv-1m`, `ohlcv-1h`.
- Prices are scaled ×1e9 and sizes ×1e6 (`Statics.PRICE_SCALING_FACTOR` / `SIZE_SCALING_FACTOR`). SBE is little-endian.
- Keys are UTC and zero-padded so they sort by time:
  - merged and final: `{streamId}/{family}/{record}/{YYYY}/{MM}/{DD}/{hh}/{mm}.zst`
  - raw collector copy: the same, plus `/{uuid}.zst`
  - venue frames (exchange-raw bucket, 30-day expiry): `{streamId}/raw/{YYYY}/{MM}/{DD}/{hh}/{mm}/{uuid}.zst`
  - `StreamEntry.parse` returns empty for any other key, and handlers skip such keys instead of failing the batch.
- A minute file is zstd over concatenated SBE messages and may mix schema versions. `StreamEntry.decode` migrates each message to the current version, so always read files through it.
- Data is keyed by `streamId`. DynamoDB rows (jobs, gaps, quality issues, statistics) are keyed by `listingId`, mapped through the registry `SecurityMaster`. An unknown stream is logged and skipped.
- Buckets per stage: `gnome-market-data-raw-<stage>` → `-merged-` → `gnome-market-data-<stage>` (final), plus `-metadata-` and `-exchange-raw-`. gnomepy's importer can write straight to the merged bucket, which triggers the same downstream steps.
- Merge: the unit of work is one merged minute.
  - The Merger lists every raw copy of that minute from the bucket, not only the copies in the batch.
  - It keeps the copy with the most records (`FullestCopyMergeStrategy`). Collector services run `desiredCount: 2` so there is a second copy to choose.
  - A re-merge overwrites the merged file.
- Transform:
  - JobCreator writes one PENDING job per target type that has a bulk converter from the collected type (`ohlcv-1h` only at minute 59).
  - Jobs use a conditional put keyed `{listingId}-{record}` + minute, and an existing job is never recreated. A re-merged minute is therefore not re-transformed automatically.
  - JobProcessor runs on a schedule per schema, takes up to 10,000 PENDING jobs, writes the final file, and marks each job COMPLETE or FAILED (one-week TTL).
  - A job whose merged minute is missing is marked COMPLETE and writes nothing.
- Gaps:
  - The gap queue has a 15-minute delivery delay, so late merges land first.
  - For each merged minute, the detector checks whether the previous minute has a transform job or a merged file. If it has neither, the detector looks back up to 2 days.
  - If nothing turns up, the detector treats the minute as the stream's first and records nothing.
  - Otherwise it writes one `UNREVIEWED`/`UNKNOWN` row per missing minute, unless the gap is shorter than `GapToleranceCalculator`'s allowance (1–1440 min, from 14 days of hourly tick counts).
  - Gap rows are `(listingId, minute)` with no record type.
- SQS handlers report batch item failures. A failed unit retries only its own messages. After 3 receives a message goes to `<queue>-dlq` and stays 14 days.
- Credentials:
  - Collectors read `gnome/market-data/<exchange>` from Secrets Manager.
  - The registry's `market-data-keys` endpoint (controller Market Data → Credentials) sets these secrets and replicates them to ap-northeast-1 and eu-west-1.
  - Collectors must never be granted `gnome/accounts/*`.
- Collector runtime:
  - Collectors download the gnome-orchestrator jar at start, pinned by `ORCHESTRATOR_VERSION` from the create/redeploy Lambda, so a task restart never drifts to a new version.
  - Until Phase 6 placement starts shards, collectors are capped at the last orchestrator release with the per-listing
    collector (`LAST_COLLECTOR_ORCHESTRATOR_VERSION` in the Lambda layer's `utils.py`): blank resolves to it and a later
    version is refused, since later releases run `ShardOrchestrator` instead.
  - The base task is 256 CPU / 512 MiB. Create and redeploy accept `cpu`/`memory` overrides.
  - The heap is fixed at 35% with ZGC, because a growing heap or G1 pauses stall socket reads on fractional vCPUs. Logging uses the non-blocking driver.
- `LAMBDAS_VERSION` (`cdk/lib/config.ts`) is part of construct IDs and function names, so changing it replaces every Java Lambda.
- The Java stacks import the `RegistryApiKeyArn`, `RegistryApiKeyId` and `UserPoolId` exports from other repos' stacks.
- These Lambdas are batch code and allocate freely. The zero-allocation hot-path rule applies to the collector (gnome-orchestrator), not here.

## Operations

- Deploys:
  - Push to `main` → `bump-and-deploy` runs the maven release (`major:` bumps major, `feat:` minor, anything else patch), publishes to GitHub Packages and tags.
  - The tag → `push-release` force-pushes `release`. `MarketDataPipeline` (InfraPipelines account) builds from `release`: Dev stage → `ApproveProd` manual step → Prod stage.
  - Each stage deploys these stacks, prefixed `Dev-` or `Prod-`: `MarketDataStorageStack`, `MarketDataEventBusStack`, `CollectorRegionalStack-{us-east-1,ap-northeast-1,eu-west-1}`, `MarketDataQualityCheckStack`, `MarketDataBackendStack`, `MarketDataTransformerStack`, `MarketDataMergerStack`, `MarketDataGapDetectorStack`, `MarketDataInventoryProcessorStack`, `MarketDataMonitoringStack`.
  - Lambdas build core from source in the pipeline. gnome-backtest picks up a core change only after a release and a version bump there.
- What runs (Java function names are `<Stage>-MarketData<X>Stack-<Name>-v1`):
  - Collectors: one ECS service `collector-<firstListingId>` per collector in `CollectorCluster-<region>`, created through the API. They write raw minutes to the raw bucket and frames to exchange-raw.
  - Merger: raw bucket → SNS → `market-data-merger-queue` (batch 1000, 3 min window) → merged bucket.
  - TransformerJobCreator: merged bucket → SNS → `market-data-transformer-queue` → `market-data-transform-jobs` table.
  - TransformerJobProcessor: EventBridge every 15 min per schema (hourly for `ohlcv-1h`). Reads merged, writes final, updates jobs.
  - GapDetector: merged → SNS → `market-data-gap-queue` → `market-data-gaps`.
  - QualityCheck: merged → SNS → `market-data-quality-check-queue` → `market-data-quality-issues` and `market-data-listing-daily-statistics`.
  - InventoryProcessor: daily S3 inventory (CSV) of the final bucket → metadata `market-data-inventory/*.csv.gz` → `market-data-inventory-queue` (batch 1) → `market-data-coverage`.
  - QualityBackfill, QualityInvestigation, BboTimeline: invoked from API routes only.
  - CollectorEcsMonitor: ECS task state changes from all regions → `collector-ecs-events` bus → status in `market-data-collectors` (ACTIVE/PENDING/FAILED).
- Logs:
  - Lambdas: `/aws/lambda/<function name>`, JSON format.
  - Collectors: `/ecs/collector-<region>` in that region, stream prefix `collector`, one-week retention.
- Alarms carry no actions. The registry alerting stack picks up their state changes via EventBridge and raises `CLOUDWATCH_ALARM`; none of them page.
  - `MarketData--<Fn>Lambda-Fault-Count-Error` for `Merger`, `TransformerJobCreator`, `TransformerJobProcessor`, `GapDetector`, `InventoryProcessor`, `QualityCheck`: any Lambda error. Read the function's log group for the stack trace.
    - JobProcessor catches per-job errors and marks the job FAILED, so this alarm misses job failures. For those, check Transform Jobs (status FAILED, `errorMessage`).
  - `MarketData--market-data-<merger|transformer|gap|quality-check|inventory>-queue-DLQ-Queue-Message-Count-Max-Error`: a message was dead-lettered. Search the logs for `Failed <unit>: ...`, fix the cause, then redrive the DLQ.
  - `MarketData--market-data-<...>-queue-Queue-Message-Age-Max-Warning` (15 min; 30 min for gap): the consumer is falling behind or failing. Check the consumer's throttles, errors and duration.
  - TODO(verify): the DLQ and queue-age alarms are uncommitted in the working tree as of 2026-10-26 and may not be deployed yet.
  - No alarm covers collector stalls, missing minutes or the API. The collector `LogErrors-<region>` metric (namespace `GnomeMarketData/Collectors`) feeds no alarm. Diagnose stalls from the Collectors page, `/ecs/collector-<region>` and the Gaps page.
- Runbooks: gnome-registry `docs/runbooks/cloudwatch/MarketData.md` covers these alarms, and `docs/runbooks/alerts/CLOUDWATCH_ALARM.md` covers the alert wrapper. This repo raises no alert types of its own.
