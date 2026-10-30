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
- Python tests (rule, missing-data and lease logic in the common layer): `python3 -m pytest cdk/lambda/tests` with pytest installed; no AWS. `test_leases_routes.py` runs the lease handler against in-memory tables and is skipped unless boto3 is installed too (e.g. a venv with `pip install pytest boto3`). `gnome-market-data-lambdas/src/test/resources/lease-constants.json` holds the lease timings the Python API and the Java reconciler share; both test suites check against it.
- Maven, not Gradle. Do not run `cdk deploy`; the pipeline deploys.

## Layout

- `gnome-market-data-core`: `StreamEntry` (S3 key layout, zstd/SBE encode and decode, listing helpers) and
  `MarketDataApi` (`collection/missing` client). Published; gnome-backtest depends on it.
- `gnome-market-data-lambdas`: all Java Lambdas, built into one image per function (the handler class is chosen by `cmd`).
  - `collection/`: the reconciler. `ReconcilerLambdaHandler` only wires.
    - `model/`: plain data (`Target`, `ShardRecord`, `Partition`, `Settings`, `Interval`, `TaskStatus`, ...).
    - `ports/`: what a tick reads and changes (`Store`, `Registry`, `Statuses`, `Services`, `Alerts`), so it runs
      the same against AWS and test fakes.
    - `reconcile/`: `Reconciler` runs one tick's steps in order over a shared `Tick`: `RuleResolver` (each rule →
      the streams it selects now), `LeaseStep` (ACTIVE session leases → rules `lease:<sessionId>`; releases and
      extends leases; the streams shards publish), `TargetsStep` (rules → targets, via `Evaluation`), `CoverageStep` (statuses → intervals and gaps, via `CoverageTracker`), `HandoffStep`,
      `AssignmentStep` (rebuilds each shard's streams/draining/cuts/publish from the targets and leases),
      `LifecycleStep` (rolls, scale-down, `min_shards` up to `max_shards`, delete), `PlacementStep` (via `Placement`), `PersistStep`, `AlertStep`.
    - `aws/`: the ports' adapters (`DynamoStore`, `S3Statuses`, `EcsServices`, `EventBridgeAlerts`,
      `RegistryAdapter`, `ServiceConfigSettings`).
  - `common/`: `Dependencies` (every client, table and env var, built lazily once per execution environment; handlers
    take what they need from it and read no env vars themselves), `SqsBatch`, `S3Utils`; `common/config/`:
    `MarketDataConfig` + `MarketDataServiceConfig` (the Service Config entry `market-data`, typed, defaults sent as
    `x-config-defaults`).
- `cdk/lib/market-data-pipeline-stack.ts`: `MarketDataPipeline` and the per-stage `AppStage`. `COLLECTION_REGIONS` = us-east-1, ap-northeast-1, eu-west-1.
- `cdk/lib/stacks/`:
  - `storage-stack`: buckets, DynamoDB tables (incl. `collection-*`; `collection-leases` has TTL on `ttl`), queues +
    DLQs, S3 wiring
  - `collection-regional-stack`: per region, cluster `CollectionCluster-<region>` in the platform network, on-demand and
    Spot capacity providers (one shard per host, host networking; `SPOT_TYPES` r7a.medium then m7a.large/m6a.large,
    lowest-price allocation), base task definition
    `collection-shard-<region>` (512 MiB `/dev/shm` for Aeron; `EcsServices` copies the container, so only shards
    started after a change get it), roles (the task role may `PutItem` `shard#*` items in us-east-1
    `service-endpoints`, gnome-shared-cdk `SERVICE_ENDPOINTS`), log group `/collection/shards-<region>`.
    `collectionNames()` must match `EcsServices`.
  - `reconciler-stack`: FIFO wake-up queue `collection-reconciler.fifo`, 1-minute schedule, the Reconciler Lambda
    (reserved concurrency 1, 1024 MB, 2-minute timeout; the queue's visibility must stay at least the timeout)
  - `backend-stack`: API Gateway + Python Lambdas behind gnome-shared-cdk's `GnomeApiAuthorizer`: people by Cognito
    ID token on every route; services by the API's own key (`market-data-api-key`, mirrored to Secrets Manager
    `gnome/api-keys/market-data`) on `collection/rules`, `collection/leases` (the registry's launcher, sessions),
    `collection/coverage/{streamId}` and `collection/missing` (backtests); bots read the last two. Publishes the API
    URL to SSM `/gnome/market-data/api-url` (read by the registry's launcher; no export, which would be a cycle)
  - `monitoring-stack`: the reconciler's fault alarm
- `cdk/lib/constructs/java-lambda.ts`: shared Java Lambda (2048 MB default, 15 min timeout, JSON logs, one-month retention). Images are pinned to
  `linux/amd64`: one built on an arm64 Mac otherwise fails with `Runtime.InvalidEntrypoint`.
- `cdk/lambda/`: Python API handlers (`functions/collection/*`) and the
  `common` layer (`utils.lambda_handler`: a `ValueError` is a 400, anything else a 500; `collection.py`
  (`wake_reconciler` is best effort: a change already stored never fails for it), `collection_rules.py`: rule types' schemas, validation,
  storage and preview selection; `coverage.py`, `missing.py`, `registry.py`; `leases.py`: lease validation, item and
  GET view; `packed.py`: packed int32 stream lists). The layer modules other than `collection.py`/`coverage.py`/
  `registry.py` import no boto3, so tests run without it.
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
- Data is keyed by `streamId`. Coverage is collection coverage only (`collection-coverage` intervals,
  `collection-gaps`, `GET collection/coverage/{streamId}`): what shards collected, not which files exist, so data
  imported by hand would not show.
- `GET collection/missing` (by `listingIds` or `streamIds`, ≤ 100, ≤ 31 days per call): per stream, every stretch no
  coverage interval covers, with the gap's reason, the target's open gap, or `NOT_COLLECTED` when nothing wanted the
  stream (common layer `missing.py`). Time before `coverageTrackedSinceNs` (`cdk/lib/config.ts`, the first coverage
  interval per stage) is `untrackedNs`: unknown, never a stretch. gnome-backtest marks these stretches as bad data
  (`MarketDataApi` in core).
- Buckets per stage: final `gnome-market-data-<stage>`, `-metadata-` (shard statuses under
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
- Consolidation (`ConsolidationStep`, Service Config `consolidation`: `enabled`, `dry_run`, `after_minutes` 15,
  `max_fill` 0.8): an ACTIVE shard whose streams would all fit on its partition's other shards, each left at most
  `max_fill` full and within connection and account limits (a dry run of `Placement`), records `mergeable_since_ns`;
  once that has held `after_minutes` it drains like a rolled shard and its streams hand off make-before-break, at the
  same `max_fill` ceiling. One merge at a time, none during a roll, a hand-off or with a stream unplaced; the
  emptiest goes first. `dry_run` only logs "Would merge shard ...".
- `min_shards` and `max_shards` (Service Config, lists of `{region, source_kind, priority, tier, count}`; tier
  defaults to `standard`, both empty by default; a malformed entry or one for a region outside `COLLECTION_REGIONS` is
  logged and skipped):
  - `min_shards`: each listed partition keeps that many ACTIVE shards that take streams, starting empty ones to make
    up the number; scale-down and consolidation never go below it.
  - `max_shards`: the most shards (any state but deleted) a listed partition runs; unlisted partitions have no cap.
    Placement never starts a shard past it, and the `min_shards` top-up stops at it (min above max: logged, max wins).
    A stream moving make-before-break off a draining shard (a roll, or a merge) may take the partition one over until
    the old shard goes. Streams that would need another shard stay unplaced (`Placement` reason `CAPACITY`) and raise
    `COLLECTION_BUDGET` keyed `COLLECTION_BUDGET:max_shards:<region>:<kind>:<priority>:<tier>` (leased ones also
    `LEASED_STREAM_UNPLACED`). During a roll at the cap, new streams wait for the old shards to go. Lowering it never
    drains running shards.
- Session leases (`collection-leases`):
  - `POST collection/leases` `{sessionId, streams:[int], mode: paper, region?}` → `{sessionId}`; idempotent per
    session (replaces the streams, ≤ 500, `expires_at` = now + 2 h, an ACTIVE lease keeps `created_at`). paper →
    RESEARCH, tier `standard`; `live` is refused (400) while live sessions read their own feeds (it would map to
    PRODUCTION). `region` is the session's launch region, stored, not checked.
  - `DELETE collection/leases/{sessionId}` → `{sessionId, status: RELEASED}` (404 if never leased); both wake the
    reconciler (best effort; the 1-minute schedule covers a lost wake-up). A DynamoDB or other AWS failure is a 500,
    which the launcher retries.
  - `GET collection/leases/{sessionId}` → `{sessionId, status, mode, priority, expiresAt, releasedAt, releaseReason,
    streams: [{streamId, state, shardId, endpoint, next}]}` (the release fields null while ACTIVE). Placement is where
    each stream is now, not where it was while the session ran (that isn't kept). `state` UNPLACED (no shard) / WARMING / LIVE: LIVE only when the shard's newest
    status, if under 45 s old, says LIVE or SUSPECT with `"published": true` for the stream, and the shard has an
    endpoint. `endpoint` `{privateIp, port}` from `service-endpoints` `shard#<id>` (null if missing, expired or not
    `state` ACTIVE: a stopping shard writes STOPPING). Mid hand-off the old (draining) shard is current and `next` is
    `{shardId, state, endpoint}` of the new one; otherwise `next` is null.
  - Shards must run an orchestrator that publishes (writes `published` in its status and its endpoint) with the
    512 MiB `/dev/shm`: a shard started before either never shows LIVE, so roll shards (`roll_to_version`) before
    sessions lease from them.
  - Item: `lease_id`, `streams` (packed like a shard's), `priority`, `tier`, `mode`, `status` ACTIVE|RELEASED,
    `created_at`/`expires_at` (epoch s), `region`; the reconciler adds `session_status`/`session_seen_at` (its last
    read of the session); released ones get `released_at`, `release_reason` (`DELETED`, `SESSION_ENDED`, `EXPIRED`)
    and `ttl` (+7 days). A malformed item is logged and skipped.
  - Each tick `LeaseStep` reads every session not yet ended, once (`GET /api/strategy-sessions?status=SUBMITTED,
    STARTING,RUNNING,STOPPING`, plus `&mode=` when the leases share one; the reconciler's `REGISTRY_API_KEY_ID`). The
    launcher writes the session row before the lease, so a holder missing from that list has ended → released
    (`SESSION_ENDED`). RUNNING → `expires_at` pushed to now + 2 h once under 1 h left; otherwise released once past
    `expires_at`. It records the status on the lease when it changes and every 5 minutes. If the registry can't be
    read, only expiry releases, and not a lease whose session was last seen STARTING or RUNNING; after 15 minutes
    unreadable it raises `LEASE_HOLDERS_UNREADABLE`. A lease's streams retire 5 minutes after it goes; its grace
    counts only if no rule wants the stream too.
  - Shard items carry `publish` (packed int32): the streams on that shard, owned or draining, that an ACTIVE lease
    wants or a lease released under 5 minutes ago wanted (so a stopping session keeps its feed while it winds down).
    The shard offers only those over Aeron.

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
  - Each stage deploys, prefixed `Dev-` or `Prod-`: `MarketDataStorageStack`, `CollectionRegionalStack-{us-east-1,ap-northeast-1,eu-west-1}`, `MarketDataReconcilerStack`, `MarketDataBackendStack`, `MarketDataMonitoringStack`.
  - The pipeline never deletes a stack removed from the app; delete it by hand once nothing imports from it.
  - Lambdas build core from source in the pipeline. gnome-backtest picks up a core change only after a release and a version bump there.
- What runs (Java function names are `<Stage>-MarketData<X>Stack-<Name>-v1`):
  - Shards: one ECS service `shard-<id>` per shard in `CollectionCluster-<region>`, created and deleted by the
    reconciler. Status every 15 s to `s3://gnome-market-data-metadata-<stage>/collection/shards/<id>/status/<task>.json`.
  - Reconciler: `collection-reconciler.fifo` (rule and lease changes, `POST collection/triggers`, the 1-minute
    schedule) → one tick.
- Logs: Lambdas `/aws/lambda/<function name>` (JSON); shards `/collection/shards-<region>` in that region, stream
  `shard/ShardContainer/<task>`, one-week retention (also `GET collection/shards/{id}/logs`).
- Alerts: the reconciler raises `COLLECTOR_SHARD_DOWN`, `STREAM_NOT_LIVE`, `STREAM_STALE`, `COLLECTION_BUDGET` (no
  account room per source, or a partition at `max_shards`), `LEASED_STREAM_UNPLACED` (a leased stream on no shard 5
  minutes after it went unplaced or was leased, whichever is later, including one that can never be placed: ended
  listing or unknown stream; one per source and region, label `streams`; sessions give up at 10) and
  `LEASE_HOLDERS_UNREADABLE` (the registry's sessions unreadable 15 minutes with leases held) on the `gnome-alerts` bus, resolving them when the condition clears. CloudWatch alarms carry no actions; the registry's
  alerting stack raises `CLOUDWATCH_ALARM` for:
  - `MarketData--ReconcilerLambda-Fault-Count-Error` (RECONCILER_FAILING): 3+ failed ticks in the period. Read
    `/aws/lambda/<Stage>-MarketDataReconcilerStack-Reconciler-v1`.
- Runbooks: gnome-registry `docs/runbooks/cloudwatch/MarketData.md` and `docs/runbooks/alerts/<TYPE>.md`.
