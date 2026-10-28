# CLAUDE.md

Historical market data. Shards (gnome-orchestrator `ShardOrchestrator`, ECS on EC2 in three regions) collect venue feeds
and write one-minute files straight to the final bucket `gnome-market-data-<stage>`, which feeds backtests and research
(gnomepy, gnome-backtest). The collection reconciler (a Java Lambda) decides what is collected where: it turns rules
into targets, places them on shards, starts and stops shard services, hands streams between shards, and records
coverage and gaps. The `market-data-api` REST API backs the controller's Market Data pages. A failure here delays or
holes historical data; live trading does not depend on it.

## Commands

- Build all: `mvn clean package` (Java 17). Spotless and checkstyle run in `validate`, so a format slip fails the build.
- Fix formatting: `mvn spotless:apply`
- All tests: `mvn test`
- One test: `mvn test -pl gnome-market-data-lambdas -am -Dtest=group.gnometrading.collection.ReconcilerTest -Dsurefire.failIfNoSpecifiedTests=false`. Use the full class name. The flag keeps `-am` from failing on core, which has no matching test.
- Lambdas only: `mvn clean package -pl gnome-market-data-lambdas -am`
- CDK: `cd cdk && npm ci && npm run build`. `npx cdk synth` needs Docker and `MAVEN_CREDENTIALS` (JSON with `GITHUB_ACTOR`/`GITHUB_TOKEN`), because every Java Lambda image builds the jar from source. There are no CDK tests.
- Python tests (rule logic in the common layer): `python3 -m pytest cdk/lambda/tests` with pytest installed; no AWS.
- Maven, not Gradle. Do not run `cdk deploy`; the pipeline deploys.

## Layout

- `gnome-market-data-core`: `StreamEntry` (S3 key layout, zstd/SBE encode and decode, listing helpers). Published; gnome-backtest depends on it.
- `gnome-market-data-lambdas`: all Java Lambdas, built into one image per function (the handler class is chosen by `cmd`).
  - `collection/`: the reconciler. `ReconcilerLambdaHandler` only wires.
    - `model/`: plain data (`Target`, `ShardRecord`, `Partition`, `Settings`, `Interval`, `TaskStatus`, ...).
    - `ports/`: what a tick reads and changes (`Store`, `Registry`, `Statuses`, `Services`, `Alerts`), so it runs
      the same against AWS and test fakes.
    - `reconcile/`: `Reconciler` runs one tick's steps in order over a shared `Tick`: `RuleResolver` (each rule →
      the streams it selects now), `TargetsStep` (rules → targets, via `Evaluation`), `CoverageStep` (statuses → intervals and gaps, via `CoverageTracker`), `HandoffStep`,
      `AssignmentStep` (rebuilds each shard's streams/draining/cuts from the targets), `LifecycleStep` (rolls,
      scale-down, delete), `PlacementStep` (via `Placement`), `PersistStep`, `AlertStep`.
    - `aws/`: the ports' adapters (`DynamoStore`, `S3Statuses`, `EcsServices`, `EventBridgeAlerts`,
      `RegistryAdapter`, `ServiceConfigSettings`).
  - `inventory/`: the daily S3 inventory of final → `market-data-coverage` (which minute files exist; not collection
    coverage).
  - `common/`: `Dependencies` (every client, table and env var, built lazily once per execution environment; handlers
    take what they need from it and read no env vars themselves), `SqsBatch`, `S3Utils`; `common/config/`:
    `MarketDataConfig` + `MarketDataServiceConfig` (the Service Config entry `market-data`, typed, defaults sent as
    `x-config-defaults`).
- `cdk/lib/market-data-pipeline-stack.ts`: `MarketDataPipeline` and the per-stage `AppStage`. `COLLECTION_REGIONS` = us-east-1, ap-northeast-1, eu-west-1.
- `cdk/lib/stacks/`:
  - `storage-stack`: buckets, DynamoDB tables (incl. `collection-*`), queues + DLQs, S3 wiring
  - `collection-regional-stack`: per region, cluster `CollectionCluster-<region>` in the platform network, on-demand and
    Spot capacity providers (one shard per host, host networking; `SPOT_TYPES` r7a.medium then m7a.large/m6a.large,
    lowest-price allocation), base task definition
    `collection-shard-<region>`, roles, log group `/collection/shards-<region>`. `collectionNames()` must match
    `EcsServices`.
  - `reconciler-stack`: FIFO wake-up queue `collection-reconciler.fifo`, 1-minute schedule, the Reconciler Lambda
    (reserved concurrency 1, 1024 MB, 2-minute timeout; the queue's visibility must stay at least the timeout)
  - `backend-stack`: API Gateway + Python Lambdas behind gnome-shared-cdk's `GnomeApiAuthorizer`: people by Cognito
    ID token on every route; services by the API's own key (`market-data-api-key`, mirrored to Secrets Manager
    `gnome/api-keys/market-data`) on `collection/rules` only
  - `inventory-processor-stack`, `monitoring-stack`
- `cdk/lib/constructs/java-lambda.ts`: shared Java Lambda (2048 MB default, 15 min timeout, JSON logs, one-month retention). Images are pinned to
  `linux/amd64`: one built on an arm64 Mac otherwise fails with `Runtime.InvalidEntrypoint`.
- `cdk/lambda/`: Python API handlers (`functions/{collection,gaps,coverage}`) and the
  `common` layer (`utils.lambda_handler`, `collection.py`, `collection_rules.py`: rule types' schemas, validation,
  storage and preview selection).
- `cdk/lib/stacks/shard-start.sh`: the shard container entrypoint. `shard-docker/` and `java-lambda-docker/` are
  written at synth time, so edit the `.ts`/`.sh` sources, not those files.

## Conventions and invariants

- Record types come from gnome-schemas `RecordType`, all in family `book`: `mbo`, `mbp-10`, `mbp-1`, `bbo-1s`, `bbo-1m`, `trades`, `ohlcv-1s`, `ohlcv-1m`, `ohlcv-1h`.
- Prices are scaled ×1e9 and sizes ×1e6 (`Statics.PRICE_SCALING_FACTOR` / `SIZE_SCALING_FACTOR`). SBE is little-endian.
- Keys are UTC and zero-padded so they sort by time:
  - final: `{streamId}/{family}/{record}/{YYYY}/{MM}/{DD}/{hh}/{mm}.zst`
  - venue frames (exchange-raw bucket, 30-day expiry): `{streamId}/raw/{YYYY}/{MM}/{DD}/{hh}/{mm}/{uuid}.zst`
  - `StreamEntry.parse` returns empty for any other key, and handlers skip such keys instead of failing the batch.
- A minute file is zstd over concatenated SBE messages and may mix schema versions. `StreamEntry.decode` migrates each message to the current version, so always read files through it.
- Data is keyed by `streamId`. The old `market-data-gaps` and `market-data-coverage` rows are keyed by `listingId`, mapped through the registry `SecurityMaster`. An unknown stream is logged and skipped.
- Buckets per stage: final `gnome-market-data-<stage>`, `-metadata-` (inventories, shard statuses under
  `collection/`, 1-day expiry there), `-exchange-raw-` (venue frames, 30-day expiry).
- Writing: each minute is written once, by one shard, with a conditional put (`If-None-Match: *`); the first writer
  keeps it. There is no merger and no raw copy.
  - A hand-off between shards cuts at a minute boundary: the new shard collects without writing (cut `WARMING`) until
    it has been LIVE for `handoff_minutes`, then the reconciler sets the same cut minute `M` (≥ 60 s ahead) on both
    shard items. The old shard writes minutes before `M`, the new one from `M`. Two minutes past `M` the old shard lets
    the stream go. If the old shard stops covering the stream first, the new one writes at once.
  - The cut is by file, not by a record's event time: a record stamped older than the previous minute lands in the
    current file. Lighter stamps per connection (~20 ms apart), so at a cut a Lighter update can be doubled or missed.
  - Not covered: a crash loses its open minute; a graceful stop writes its partial minute, and a replacement starting
    within that minute gets 412 for it.
- Reconciler: the only writer of `collection-targets` and `collection-shards`. Every tick is full and idempotent, and
  every shard write is conditional on `generation`. Settings come from the Service Config `market-data`
  (`MarketDataConfig`); a failed read keeps the last good one, or the defaults.
- SQS handlers report batch item failures. A failed unit retries only its own messages. After 3 receives a message goes to `<queue>-dlq` and stays 14 days.
- Credentials: shards read `gnome/market-data/<source>/<label>` (their item's `accounts`), set through the registry's
  market-data accounts. Shards must never be granted `gnome/accounts/*`.
- Shard runtime: shards download the gnome-orchestrator jar at start, pinned by `ORCHESTRATOR_VERSION` in their task
  definition (the reconciler registers one per shard from the base), so a restart never drifts. New versions reach
  shards only through a roll (`roll_to_version`). Heap 35% with ZGC; the non-blocking log driver.
- `LAMBDAS_VERSION` (`cdk/lib/config.ts`) is part of construct IDs and function names, so changing it replaces every Java Lambda.
- The registry key comes only from `REGISTRY_API_KEY_ID` (the registry's `RegistryApiKeyId` export); `market-data.<stage>.properties` holds just `registry.url`.
- The Java stacks import the `RegistryApiKeyArn`, `RegistryApiKeyId` and `UserPoolId` exports from other repos' stacks, and the reconciler stack `ControllerApiUrl` and `ControllerServiceConfigApiKeyId` (gnome-controller).
- These Lambdas are batch code and allocate freely. The zero-allocation hot-path rule applies to the shards (gnome-orchestrator), not here.

- Collection coverage: a stream's open interval is extended in `market-data-collection-coverage` only every 10
  minutes or at UTC midnight (`CoverageTracker.EXTEND_EVERY_NANOS`), not every tick; it is closed at the last status
  that still covered the stream (`TASK_STOPPED` if the shard still assigns it, `REMOVED` otherwise).
- `TargetsStep` caches registry stream info for 5 minutes; streams whose source retires them with no grace are
  re-read every tick. Stream liveness is the registry's `COALESCE(stream.active, listing.active)`.
- During a roll (`roll_to_version` set), placement only uses shards already on that version.

- Rule types: `static_streams` (stream ids), `event` (event ids: every active stream of each event, both sides of
  every contract), `event_filter` (exchanges/categories/tags/expiry window, optional `min_volume_24h` and `top_n`,
  required `max_streams`). The Python schemas (`collection_rules.py`) and the Java reader/resolver (`RuleItems`,
  `RuleResolver`) must agree. An `event` rule keeps its ids in a top-level number set `event_ids` (not in
  `parameters`) so services add and remove them atomically (`PATCH add_event_ids` / `remove_event_ids`); the
  reconciler removes resolved events. A rule with `managed_by` belongs to a system: people may rename, pause or delete
  it but not change what it selects. Event rules resolve every 5 minutes (or at once on a parameter change); a volume
  rule's band (stay within 1.5x `top_n` / half `min_volume_24h`) uses the rule's current targets, not stored state.
  The reconciler writes `resolved_events`, `resolved_streams`, `waiting_events`, `capped`, `resolved_at_ns` on the
  rule item.

## Operations

- Deploys:
  - Push to `main` → `bump-and-deploy` runs the maven release (`major:` bumps major, `feat:` minor, anything else patch), publishes to GitHub Packages and tags.
  - The tag → `push-release` force-pushes `release`. `MarketDataPipeline` (InfraPipelines account) builds from `release`: Dev stage → `ApproveProd` manual step → Prod stage.
  - Each stage deploys, prefixed `Dev-` or `Prod-`: `MarketDataStorageStack`, `CollectionRegionalStack-{us-east-1,ap-northeast-1,eu-west-1}`, `MarketDataReconcilerStack`, `MarketDataBackendStack`, `MarketDataInventoryProcessorStack`, `MarketDataMonitoringStack`.
  - The pipeline never deletes a stack removed from the app; delete it by hand once nothing imports from it.
  - Lambdas build core from source in the pipeline. gnome-backtest picks up a core change only after a release and a version bump there.
- What runs (Java function names are `<Stage>-MarketData<X>Stack-<Name>-v1`):
  - Shards: one ECS service `shard-<id>` per shard in `CollectionCluster-<region>`, created and deleted by the
    reconciler. Status every 15 s to `s3://gnome-market-data-metadata-<stage>/collection/shards/<id>/status/<task>.json`.
  - Reconciler: `collection-reconciler.fifo` (rule changes, `POST collection/triggers`, the 1-minute schedule) → one tick.
  - InventoryProcessor: daily S3 inventory (CSV) of the final bucket → metadata `market-data-inventory/*.csv.gz` → `market-data-inventory-queue` (batch 1) → `market-data-coverage`.
- Logs: Lambdas `/aws/lambda/<function name>` (JSON); shards `/collection/shards-<region>` in that region, stream
  `shard/ShardContainer/<task>`, one-week retention (also `GET collection/shards/{id}/logs`).
- Alerts: the reconciler raises `COLLECTOR_SHARD_DOWN`, `STREAM_NOT_LIVE`, `STREAM_STALE`, `COLLECTION_BUDGET` on the
  `gnome-alerts` bus, resolving them when the condition clears. CloudWatch alarms carry no actions; the registry's
  alerting stack raises `CLOUDWATCH_ALARM` for:
  - `MarketData--ReconcilerLambda-Fault-Count-Error` (RECONCILER_FAILING): 3+ failed ticks in the period. Read
    `/aws/lambda/<Stage>-MarketDataReconcilerStack-Reconciler-v1`.
  - `MarketData--InventoryProcessorLambda-…`: any Lambda error.
  - `MarketData--market-data-inventory-queue-DLQ-…` / `-Queue-Message-Age-…`: dead-lettered or stale work.
- Runbooks: gnome-registry `docs/runbooks/cloudwatch/MarketData.md` and `docs/runbooks/alerts/<TYPE>.md`.
