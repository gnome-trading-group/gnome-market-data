# Collector validation tools

`collector_acceptance.py` produces a machine-readable JSON report and a Markdown handoff report for a one- or multi-listing collector window. It validates the full sandbox path:

- ECS service and task health
- lossless raw manifests, checksums, record framing, coverage, and JSON payloads
- normalized zstd/SBE files, identifiers, timestamps, prices, and books
- event-timestamp parity between actionable raw venue messages and normalized MBP-10 records
- immutable contract/event/relationship metadata and per-listing coverage
- receive time, event time, MBP-10 depth, and venue sequence-gap evidence per security
- 30-day bucket retention, the $100 monthly budget, and its email alert thresholds

The tool is read-only against AWS. Downloads are staged in a temporary directory and removed when the report completes. Reports are written below the ignored `build/collector-acceptance` directory by default.

## Requirements

- AWS CLI profile with read access to the sandbox stack, ECS service, CloudWatch metadata, and both S3 buckets
- Java 17
- Python 3.10 or newer
- `zstd` command-line utility
- the sandbox collector JAR at `cdk/docker/sandbox-collector/app.jar` (or supplied with `--collector-jar`)

## Run a completed one-hour window

```bash
python3 tools/collector_acceptance.py \
  --profile gnome-sandbox \
  --region us-east-1 \
  --stack GnomePolymarketSandbox \
  --start 2026-08-05T22:42:33Z \
  --duration-minutes 60
```

The default two-minute settlement delay allows final minute objects to reach S3. A gating report started before settlement fails immediately and prints the earliest valid retry time. For a non-gating progress report, add `--allow-incomplete`.

The validator uses the most recent book snapshot in a 60-minute pre-window context to resolve the subscribed Polymarket outcome. Increase `--context-lookback-minutes` for a long-lived connection whose latest book snapshot is older, or supply the known public outcome token with `--subscribed-asset-id`. The report fails rather than counting every outcome when the subscribed asset cannot be resolved.

For a concurrent related-listing run, the immutable collection metadata normally resolves every outcome token. Override a listing explicitly only when investigating metadata with `--subscribed-asset LISTING_ID=ASSET_ID`; repeat the option for more than one listing. Raw/normalized parity is keyed by security ID plus event time so simultaneous events on related contracts cannot mask a missing or misrouted message.

## Deterministic local replay

The replay fixture contains synthetic identifiers and live-shaped Polymarket payloads. It covers an array book snapshot, `PONG`, a multi-outcome price change, and a trade. The verification command builds GNOMERAW v1, replays every frame through the production `PolymarketSocketReader` from the collector JAR, and compares the emitted MBP-10 messages with the checked-in expectations.

```bash
PYTHONPATH=tools python3 -B tools/raw_replay.py verify \
  tools/fixtures/polymarket-replay.json
```

Build or inspect a capture independently:

```bash
PYTHONPATH=tools python3 -B tools/raw_replay.py build \
  tools/fixtures/polymarket-replay.json build/polymarket-replay.raw.zst

PYTHONPATH=tools python3 -B tools/raw_replay.py inspect \
  build/polymarket-replay.raw.zst --emit
```

`--speed 0` replays immediately. `--speed 1` preserves the recorded receive-time spacing; higher values accelerate it proportionally. Replay is an offline path, so its diagnostic allocations are intentionally outside the production hot-path rule.
