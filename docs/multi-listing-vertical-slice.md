# Multi-listing collection and graph-backtest contract

Status: Implemented locally; AWS deployment intentionally deferred until the active collection window ends and deployment is approved.

## Collection model

The collector accepts `LISTINGS` as a comma-separated list of canonical registry listing IDs. The legacy `LISTING` value remains a fallback for a single listing. One ECS task creates an independent gateway, normalized collector, raw-frame ring, and health source for every listing.

This isolation prevents one order book from contaminating another while keeping receive timestamps comparable because all pipelines use the same host epoch clock. Overall task health fails if any configured listing has no normalized data, stale raw ingress, or an archival failure.

## Preserved data contract

Before sockets start, the collector writes the same immutable `v1/collections/<collection-id>/contract-metadata.json` object to both data buckets. It contains:

- listing, exchange, security, listing-spec, event, and outcome metadata;
- every registry relationship touching a selected security;
- normalized and raw path prefixes;
- the event-time, receive-time, sequence, and MBP-10 depth semantics.

Each raw minute manifest references the collection ID and metadata key. Normalized SBE messages remain byte-for-byte records containing `timestampEvent`, `timestampRecv`, exchange/security identifiers, venue sequence, update depth, and all ten bid/ask levels.

Polymarket currently publishes no supported order-book sequence. Its SBE sequence therefore remains the schema null value (`0`). The validator derives gaps only from non-null venue sequences and never invents continuity for a venue that does not provide it.

## Next acceptance test

Select a small related set—normally both outcomes of one binary event—and set the stack's `ListingIds` parameter to their comma-separated IDs. Keep `ListingId` only as the backwards-compatible single-listing parameter. Numeric registry IDs are mutable operational references, not permanent market identity; immediately before deployment, re-query every listing and require its security, condition, token, active state, and listing spec to match the frozen cohort.

The current candidate is frozen in [`lck-2026-calibration-cohort.yaml`](./lck-2026-calibration-cohort.yaml). It remains `PENDING_REVIEW`: its three outcomes are pairwise mutually exclusive but are not a collectively exhaustive set, and live CLOB verification must pass before deployment.

After an approved deployment, start a fresh window after ECS reaches one healthy task and the rollout is complete. Run:

```bash
python3 tools/collector_acceptance.py \
  --profile gnome-sandbox \
  --region us-east-1 \
  --stack GnomePolymarketSandbox \
  --start <UTC-START> \
  --duration-minutes 60
```

The report must prove:

- the captured listing IDs exactly match the CloudFormation parameter;
- every listing has minute coverage and satisfies the receive-gap limit independently;
- one consistent metadata snapshot contains all selected listings and their graph edges;
- normalized security IDs exactly match raw manifests;
- event time, receive time, depth, and ten-level book fields decode for every record;
- raw/normalized parity matches by `(security ID, event timestamp)`, so simultaneous related events cannot hide a missing or misrouted message;
- sequence gaps are reported per security when the venue supplies sequence numbers;
- no individual listing can be stale while aggregate task health passes.

## Graph replay ordering

Backtests must consume records in receive-time order to preserve causality and avoid using information before this collector could have observed it. Event time remains attached for venue chronology, latency measurement, and watermarking. For deterministic ties, order by receive time, security ID, then the original per-listing record order.

Graph vertices come from the captured security/event-contract metadata. Graph edges come from the captured registry relationships rather than the live registry, ensuring a historical run cannot change when registry data is later edited.
