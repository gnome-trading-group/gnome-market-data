# Phase 0 exit criteria

Status: Complete
Phase objective: prove that one Polymarket listing can be collected continuously, stored losslessly, normalized correctly, and replayed deterministically in the isolated AWS sandbox.

## Formal acceptance window

The task began running before the ECS rolling deployment finished. Because the old and new tasks overlapped safely during deployment, the single-writer acceptance window begins only after the stack reached steady state.

- Start: `2026-08-05T22:42:33Z` (`3:42:33 PM` Arizona)
- One-hour end: `2026-08-05T23:42:33Z` (`4:42:33 PM` Arizona)
- Report eligibility after two-minute settlement: `2026-08-05T23:44:33Z`
- Changes prohibited during the window: collector deployments, task scaling, listing changes, parser changes, bucket lifecycle changes, and manual task restarts

### Formal result

The settled one-hour report passed every automated data, runtime, latency, retention, and budget check:

- 3,049 raw records with a 10.4-second maximum receive gap
- 647 actionable raw events and 647 normalized messages, with zero missing or extra messages
- 191.685 ms p95 and 772.66 ms maximum event-to-receive latency, with no negative or over-one-second events
- one healthy ECS task for the full window, with zero replacements or stopped same-revision tasks
- 30-day expiration on both raw buckets and a $100 monthly budget with email alerts at $10, $25, $50, and $100
- deterministic local replay passed with stable raw and reader-output digests

Phase 0 remains open until the controlled-restart and ten-minute post-restart gates in section 6 pass.

### Controlled-restart result

The controlled task replacement and recovery gates passed:

- Restart requested at `2026-08-06T01:23:53Z`; the new revision-5 task started at `01:24:27.874Z`.
- ECS returned to one healthy task, zero pending tasks, zero failed tasks, and a completed rollout before the clean-window boundary at `01:27:22Z`.
- The new task logged `SOCKET_CONNECTED`, started the configured listing, and uploaded both lossless raw and normalized MBP-10 objects.
- Its first raw batch contained nine monotonic records and began with an array-form `book` snapshot.
- The first normalized object contained seven valid records; its first record was a non-crossed, valid-price `Modify` with stable identifiers and a null sequence.
- The accepted post-restart window (`01:37:22Z–01:47:22Z`) passed every automated gate: 430 raw records, 214 actionable events, 214 normalized messages, zero missing or extra messages, 146.858 ms p95 latency, 230.77 ms maximum latency, and no restarts or latency outliers.

An earlier post-restart candidate window contained one 1,127.764 ms `price_change` outlier. Its raw ingress and normalized receive timestamps were identical, proving normalization added no delay; the latency accrued before the application received the venue message. The hard one-second gate remains unchanged, and the following consecutive window passed it.

That investigation also found a validator limitation: a five-minute context no longer reached the restart snapshot and caused both Polymarket outcomes to be counted. The validator now requires a resolved subscribed asset and uses a configurable 60-minute snapshot lookback, failing explicitly if the asset cannot be resolved.

Phase 0 is complete. The next gate is a 24-hour uninterrupted collection window, followed by seven days if the 24-hour report passes.

## Required gates

Every required gate must pass before Phase 0 is complete.

### 1. Runtime stability

- CloudFormation is `UPDATE_COMPLETE`.
- ECS has exactly one desired and one running task, with zero pending tasks.
- The active deployment is `COMPLETED` with zero failed tasks.
- The task is `HEALTHY` for the whole single-writer window.
- The active task revision has zero restarts during the window.
- No credential, API key, secret value, or protected registry URL appears in reports or logs.

### 2. Lossless raw capture

- At least one raw record is present in every minute between the first and last record.
- Maximum receive-time gap is at most 30 seconds while the WebSocket is connected.
- Every selected manifest parses and references an existing object.
- Every compressed object matches its manifest SHA-256 checksum.
- Manifest message count, listing ID, first timestamp, and last timestamp match the decoded object.
- Raw record timestamps are monotonic within each object.
- Every non-control payload is valid UTF-8 JSON; application `PONG` frames remain exact control payloads.
- No frame is truncated, silently dropped, or overwritten.

### 3. Normalized data correctness

- Every normalized object is valid zstd containing whole MBP-10 messages.
- Exchange and security identifiers are present and stable.
- Every record has a positive receive timestamp.
- Prices remain within the prediction-market range and no published book is crossed.
- Polymarket sequence numbers remain null unless the venue begins publishing a supported sequence.
- Every actionable raw `book`, subscribed-outcome `price_change`, and subscribed-outcome `last_trade_price` event has a normalized output with the same venue timestamp.
- Every normalized record has a corresponding actionable raw event.
- The deployed reader has separately demonstrated an array-form book snapshot flowing into normalized MBP-10 data.

### 4. Latency baseline

- Event-to-receive latency is reported with minimum, median, p95, and maximum values.
- No steady-state event has a negative latency.
- Steady-state p95 is at most 500 ms for this research sandbox.
- Any event above one second is identified and explained before Phase 0 closes. Initial or reconnect book snapshots are reported separately because their venue timestamp can predate receipt.

This is an R&D data-quality threshold, not a production market-making latency objective.

### 5. Deterministic replay

- The checked-in GNOMERAW fixture has a stable frame-sequence digest.
- Replay preserves frame order, receive timestamps, and payload bytes.
- The production Polymarket reader emits the expected book update, price update, and trade.
- Emitted MBP-10 bytes have a stable digest.
- The fixture covers an array snapshot, application `PONG`, a multi-outcome price-change payload, filtering to the subscribed outcome, and a trade.

### 6. Operations and recovery

- Thirty-day lifecycle retention remains configured for both raw buckets.
- Budget alerts remain configured and the sandbox stays within the approved monthly budget.
- After the one-hour report passes, perform one controlled task restart.
- Following restart, prove a new socket connection, array book snapshot, raw upload, normalized upload, and healthy state.
- Run a separate ten-minute post-restart acceptance report with no missing or extra normalized events.

The controlled restart occurs after the formal hour and therefore cannot invalidate it.

## Raw capture isolation boundary

The local next-version implementation now hands raw frames from the socket reader to a preallocated bounded ring without allocating on the successful ingress path. Compression and S3 upload remain on background threads in the same JVM. This is appropriate for research collection, but it still blocks co-locating archival with latency-sensitive trading until the worker moves to a sidecar and the allocation/soak gates in [Zero-GC raw capture design](zero-gc-raw-capture.md) pass.

## Automated evidence

Run the formal report after the settlement time:

```bash
python3 tools/collector_acceptance.py \
  --profile gnome-sandbox \
  --region us-east-1 \
  --stack GnomePolymarketSandbox \
  --start 2026-08-05T22:42:33Z \
  --duration-minutes 60 \
  --settle-seconds 120
```

Verify replay independently:

```bash
PYTHONPATH=tools python3 -B tools/raw_replay.py verify \
  tools/fixtures/polymarket-replay.json
```

The JSON report is the machine-readable evidence. The Markdown report is the human review artifact. Both are generated below the ignored `build/collector-acceptance` directory unless another output directory is supplied.

## Decision rule

| Result | Meaning | Next action |
|---|---|---|
| PASS | All one-hour, replay, and controlled-restart gates pass | Begin a 24-hour uninterrupted collection window |
| CONDITIONAL | Data gates pass but a documented latency outlier or operational warning remains | Resolve or explicitly accept the warning before 24 hours |
| FAIL | Integrity, coverage, parity, health, or replay gate fails | Stay in Phase 0, diagnose, restart a new clean hour |

After a successful 24-hour window, expand to seven days and use the replayed normalized data to begin the first backtest harness. Paper-trading work does not begin until the dataset and replay gates remain stable across that longer window.
