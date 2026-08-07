#!/usr/bin/env python3
"""Validate a one- or multi-listing collector window from ECS through raw and normalized S3 data."""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import datetime as dt
import hashlib
import json
import shutil
import statistics
import struct
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterable


RAW_HEADER = struct.Struct(">8sII")
RAW_RECORD_HEADER = struct.Struct(">qI")
NANOS_PER_SECOND = 1_000_000_000
NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND
PRICE_SCALE = 1_000_000_000
SANDBOX_BUDGET_NAME = "gnome-polymarket-sandbox"
REQUIRED_RETENTION_DAYS = 30
REQUIRED_BUDGET_THRESHOLDS = {10.0, 25.0, 50.0, 100.0}
DOWNLOAD_WORKERS = 16


_OBJECT_CACHE: dict[tuple[str, str], Path] = {}


class AcceptanceError(RuntimeError):
    """Raised when the report cannot be produced."""


def run(command: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise AcceptanceError(f"Command failed ({command[0]}): {detail}")
    return result


def parse_json_output(output: str) -> Any:
    start = min((index for index in (output.find("{"), output.find("[")) if index >= 0), default=-1)
    if start < 0:
        raise AcceptanceError("Command did not return JSON")
    return json.loads(output[start:])


def aws_json(profile: str, region: str, *arguments: str) -> Any:
    result = run([
        "aws",
        *arguments,
        "--profile",
        profile,
        "--region",
        region,
        "--output",
        "json",
    ])
    return parse_json_output(result.stdout)


def parse_timestamp(value: str) -> dt.datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = dt.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("Timestamp must include a timezone")
    return parsed.astimezone(dt.timezone.utc)


def parse_subscribed_asset(value: str) -> tuple[int, str]:
    listing_id, separator, asset_id = value.partition("=")
    if not separator or not listing_id.isdigit() or not asset_id:
        raise argparse.ArgumentTypeError("Expected LISTING_ID=ASSET_ID")
    return int(listing_id), asset_id


def iso_time(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[int((len(ordered) - 1) * fraction)]


def summary(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "median": None, "p95": None, "max": None}
    return {
        "count": len(values),
        "min": round(min(values), 3),
        "median": round(statistics.median(values), 3),
        "p95": round(percentile(values, 0.95), 3),
        "max": round(max(values), 3),
    }


def stack_context(profile: str, region: str, stack: str) -> dict[str, str]:
    described = aws_json(profile, region, "cloudformation", "describe-stacks", "--stack-name", stack)
    stacks = described.get("Stacks", [])
    if len(stacks) != 1:
        raise AcceptanceError(f"Expected one stack named {stack}")
    outputs = {entry["OutputKey"]: entry["OutputValue"] for entry in stacks[0].get("Outputs", [])}
    required = {"VenueRawBucketName", "NormalizedRawBucketName", "CollectorServiceName"}
    missing = sorted(required - outputs.keys())
    if missing:
        raise AcceptanceError(f"Stack is missing outputs: {', '.join(missing)}")

    resources = aws_json(
        profile,
        region,
        "cloudformation",
        "describe-stack-resources",
        "--stack-name",
        stack,
    ).get("StackResources", [])
    clusters = [resource["PhysicalResourceId"] for resource in resources if resource["ResourceType"] == "AWS::ECS::Cluster"]
    if len(clusters) != 1:
        raise AcceptanceError("Expected exactly one ECS cluster in the sandbox stack")
    outputs["CollectorClusterName"] = clusters[0]
    outputs["StackStatus"] = stacks[0]["StackStatus"]
    parameters = {entry["ParameterKey"]: entry["ParameterValue"] for entry in stacks[0].get("Parameters", [])}
    outputs["ConfiguredListingIds"] = parameters.get("ListingIds") or parameters.get("ListingId", "")
    return outputs


def list_objects(profile: str, region: str, bucket: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    continuation_token: str | None = None
    while True:
        arguments = ["s3api", "list-objects-v2", "--bucket", bucket]
        if continuation_token:
            arguments.extend(["--continuation-token", continuation_token])
        response = aws_json(profile, region, *arguments)
        objects.extend(response.get("Contents", []))
        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            return objects


def modified_at(item: dict[str, Any]) -> dt.datetime:
    return parse_timestamp(item["LastModified"])


def _download_object_from_s3(
    profile: str,
    region: str,
    bucket: str,
    key: str,
    destination: Path,
) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    run([
        "aws",
        "s3api",
        "get-object",
        "--bucket",
        bucket,
        "--key",
        key,
        str(destination),
        "--profile",
        profile,
        "--region",
        region,
        "--output",
        "json",
    ])


def download_object(
    profile: str,
    region: str,
    bucket: str,
    key: str,
    destination: Path,
) -> None:
    cached = _OBJECT_CACHE.get((bucket, key))
    if cached is None:
        _download_object_from_s3(profile, region, bucket, key, destination)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(cached, destination)


def prefetch_objects(
    profile: str,
    region: str,
    bucket: str,
    objects: list[dict[str, Any]],
    destination: Path,
) -> None:
    if not objects:
        return
    destination.mkdir(parents=True, exist_ok=True)

    def fetch(index_and_item: tuple[int, dict[str, Any]]) -> tuple[str, Path]:
        index, item = index_and_item
        key = str(item["Key"])
        cached = destination / str(index)
        _download_object_from_s3(profile, region, bucket, key, cached)
        return key, cached

    workers = min(DOWNLOAD_WORKERS, len(objects))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for key, cached in executor.map(fetch, enumerate(objects)):
            _OBJECT_CACHE[(bucket, key)] = cached


def decompress_zstd(source: Path) -> bytes:
    result = subprocess.run(["zstd", "-dc", str(source)], capture_output=True)
    if result.returncode != 0:
        raise AcceptanceError(f"Unable to decompress {source.name}")
    return result.stdout


def iter_raw_records(data: bytes) -> Iterable[tuple[int, bytes, int]]:
    if len(data) < RAW_HEADER.size:
        raise AcceptanceError("Raw object is smaller than its header")
    magic, version, listing_id = RAW_HEADER.unpack_from(data, 0)
    if magic != b"GNOMERAW" or version != 1:
        raise AcceptanceError("Raw object has an unsupported header")
    offset = RAW_HEADER.size
    while offset < len(data):
        if len(data) - offset < RAW_RECORD_HEADER.size:
            raise AcceptanceError("Raw object has a truncated record header")
        receive_timestamp, length = RAW_RECORD_HEADER.unpack_from(data, offset)
        offset += RAW_RECORD_HEADER.size
        payload = data[offset : offset + length]
        if len(payload) != length:
            raise AcceptanceError("Raw object has a truncated payload")
        offset += length
        yield receive_timestamp, payload, listing_id


def event_objects(decoded: Any) -> Iterable[dict[str, Any]]:
    values = decoded if isinstance(decoded, list) else [decoded]
    return (value for value in values if isinstance(value, dict))


def raw_report(
    profile: str,
    region: str,
    bucket: str,
    objects: list[dict[str, Any]],
    start_ns: int,
    end_ns: int,
    context_lookback_ns: int,
    configured_subscribed_assets: dict[int, str],
    legacy_subscribed_asset: str | None,
    work: Path,
) -> tuple[dict[str, Any], collections.Counter[tuple[int, int]]]:
    manifests = [item for item in objects if item["Key"].endswith(".manifest.json")]
    manifest_errors: list[str] = []
    checksums_passed = 0
    window_records: list[tuple[int, bytes, int, int]] = []
    context_records: list[tuple[int, bytes, int, int]] = []
    context_start_ns = start_ns - context_lookback_ns
    listing_security_ids: dict[int, int] = {}
    metadata_assets: dict[int, str] = {}
    metadata_listing_ids: set[int] = set()
    metadata_keys: set[str] = set()
    collection_ids: set[str] = set()
    selected_manifest_count = 0

    for index, item in enumerate(sorted(manifests, key=lambda value: value["Key"])):
        manifest_path = work / "raw" / "manifests" / f"{index}.json"
        download_object(profile, region, bucket, item["Key"], manifest_path)
        try:
            manifest = json.loads(manifest_path.read_text())
            first = int(manifest["firstReceiveTimestampNanos"])
            last = int(manifest["lastReceiveTimestampNanos"])
            if last < context_start_ns or first >= end_ns:
                continue
            selected_manifest_count += 1
            intersects_window = last >= start_ns and first < end_ns
            listing_id = int(manifest["listingId"])
            security_id = int(manifest["securityId"])
            if intersects_window:
                previous_security_id = listing_security_ids.get(listing_id)
                if previous_security_id is not None and previous_security_id != security_id:
                    manifest_errors.append("listing security ID mismatch")
                listing_security_ids[listing_id] = security_id
                metadata_key = str(manifest.get("contractMetadataKey", ""))
                collection_id = str(manifest.get("collectionId", ""))
                if not metadata_key or not collection_id or collection_id == "untracked":
                    manifest_errors.append("missing contract metadata reference")
                else:
                    metadata_keys.add(metadata_key)
                    collection_ids.add(collection_id)
            data_path = work / "raw" / "objects" / f"{index}.raw.zst"
            download_object(profile, region, bucket, manifest["dataKey"], data_path)
            compressed = data_path.read_bytes()
            if hashlib.sha256(compressed).hexdigest() != manifest["sha256"]:
                manifest_errors.append("checksum mismatch")
                continue
            checksums_passed += 1
            records = list(iter_raw_records(decompress_zstd(data_path)))
            if len(records) != int(manifest["messageCount"]):
                manifest_errors.append("message count mismatch")
            if [record[0] for record in records] != sorted(record[0] for record in records):
                manifest_errors.append("non-monotonic object timestamps")
            if records and records[0][0] != first:
                manifest_errors.append("first timestamp mismatch")
            if records and records[-1][0] != last:
                manifest_errors.append("last timestamp mismatch")
            if any(listing != int(manifest["listingId"]) for _, _, listing in records):
                manifest_errors.append("listing ID mismatch")
            for receive_timestamp, payload, _ in records:
                if start_ns <= receive_timestamp < end_ns:
                    window_records.append((receive_timestamp, payload, listing_id, security_id))
                if context_start_ns <= receive_timestamp < end_ns:
                    context_records.append((receive_timestamp, payload, listing_id, security_id))
        except (AcceptanceError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            manifest_errors.append("invalid manifest or raw object")

    for index, metadata_key in enumerate(sorted(metadata_keys)):
        metadata_path = work / "raw" / "metadata" / f"{index}.json"
        try:
            download_object(profile, region, bucket, metadata_key, metadata_path)
            metadata = json.loads(metadata_path.read_text())
            if str(metadata.get("collectionId", "")) not in collection_ids:
                manifest_errors.append("contract metadata collection ID mismatch")
            for listing in metadata.get("listings", []):
                metadata_listing_ids.add(int(listing["listingId"]))
                exchange_security_id = str(listing.get("exchangeSecurityId", ""))
                if exchange_security_id:
                    metadata_assets[int(listing["listingId"])] = exchange_security_id.rsplit(":", 1)[-1]
        except (AcceptanceError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            manifest_errors.append("invalid contract metadata")

    window_records.sort(key=lambda value: (value[0], value[2]))
    payload_shapes: collections.Counter[str] = collections.Counter()
    event_types: collections.Counter[str] = collections.Counter()
    decoded_events: list[tuple[int, int, int, dict[str, Any]]] = []
    json_errors = 0
    for receive_timestamp, payload, listing_id, security_id in window_records:
        try:
            text = payload.decode("utf-8")
            if text == "PONG":
                payload_shapes["PONG"] += 1
                continue
            decoded = json.loads(text)
            payload_shapes["array" if isinstance(decoded, list) else "object"] += 1
            for event in event_objects(decoded):
                event_types[str(event.get("event_type", "unknown"))] += 1
                decoded_events.append((receive_timestamp, listing_id, security_id, event))
        except (UnicodeDecodeError, json.JSONDecodeError):
            json_errors += 1

    context_events: list[tuple[int, dict[str, Any]]] = []
    for _, payload, listing_id, _ in context_records:
        try:
            text = payload.decode("utf-8")
            if text != "PONG":
                context_events.extend((listing_id, event) for event in event_objects(json.loads(text)))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    discovered_assets: dict[int, str] = {}
    for listing_id in listing_security_ids:
        candidates = [
            str(event["asset_id"])
            for event_listing_id, event in context_events
            if event_listing_id == listing_id
            and event.get("event_type") == "book"
            and event.get("asset_id") is not None
        ]
        if candidates:
            discovered_assets[listing_id] = collections.Counter(candidates).most_common(1)[0][0]
    if legacy_subscribed_asset and len(listing_security_ids) == 1:
        configured_subscribed_assets = {
            next(iter(listing_security_ids)): legacy_subscribed_asset,
            **configured_subscribed_assets,
        }
    subscribed_assets: dict[int, str] = {}
    subscribed_asset_sources: dict[int, str] = {}
    for listing_id in listing_security_ids:
        if listing_id in configured_subscribed_assets:
            subscribed_assets[listing_id] = configured_subscribed_assets[listing_id]
            subscribed_asset_sources[listing_id] = "configured"
        elif listing_id in metadata_assets:
            subscribed_assets[listing_id] = metadata_assets[listing_id]
            subscribed_asset_sources[listing_id] = "contract-metadata"
        elif listing_id in discovered_assets:
            subscribed_assets[listing_id] = discovered_assets[listing_id]
            subscribed_asset_sources[listing_id] = "book"

    actionable: collections.Counter[tuple[int, int]] = collections.Counter()
    actionable_latencies: list[float] = []
    actionable_outliers: list[dict[str, Any]] = []
    incremental_latencies: list[float] = []
    incremental_outliers: list[dict[str, Any]] = []
    snapshot_ages: list[float] = []
    snapshot_age_outliers: list[dict[str, Any]] = []
    for receive_timestamp, listing_id, security_id, event in decoded_events:
        event_type = event.get("event_type")
        timestamp = event.get("timestamp")
        if timestamp is None:
            continue
        subscribed_asset = subscribed_assets.get(listing_id)
        if subscribed_asset is None:
            continue
        timestamp_ns = int(timestamp) * 1_000_000
        actionable_count = 0
        if event_type == "book" and str(event.get("asset_id")) == subscribed_asset:
            actionable_count = 1
        elif event_type == "last_trade_price" and str(event.get("asset_id")) == subscribed_asset:
            actionable_count = 1
        elif event_type == "price_change":
            changes = event.get("price_changes")
            if isinstance(changes, list):
                matching = [
                    change
                    for change in changes
                    if str(change.get("asset_id")) == subscribed_asset
                ]
                actionable_count = len(matching)
            elif str(event.get("asset_id")) == subscribed_asset:
                actionable_count = 1
        if actionable_count:
            actionable[(security_id, timestamp_ns)] += actionable_count
            latency_ms = (receive_timestamp - timestamp_ns) / 1_000_000
            actionable_latencies.extend([latency_ms] * actionable_count)
            classified_latencies = snapshot_ages if event_type == "book" else incremental_latencies
            classified_latencies.extend([latency_ms] * actionable_count)
            if latency_ms > 1_000:
                outlier = {
                    "eventTimestamp": timestamp_ns,
                    "receiveTimestamp": receive_timestamp,
                    "latencyMs": round(latency_ms, 3),
                    "eventType": event_type,
                    "listingId": listing_id,
                    "securityId": security_id,
                    "actionableCount": actionable_count,
                }
                actionable_outliers.append(outlier)
                (snapshot_age_outliers if event_type == "book" else incremental_outliers).append(outlier)

    timestamps = [timestamp for timestamp, _, _, _ in window_records]
    per_listing: dict[str, dict[str, Any]] = {}
    all_missing_minutes: list[tuple[int, int]] = []
    all_gaps: list[float] = []
    for listing_id, security_id in sorted(listing_security_ids.items()):
        listing_timestamps = [
            timestamp
            for timestamp, _, record_listing_id, _ in window_records
            if record_listing_id == listing_id
        ]
        minute_counts = collections.Counter(timestamp // NANOS_PER_MINUTE for timestamp in listing_timestamps)
        missing_minutes = [
            minute
            for minute in range(min(minute_counts), max(minute_counts) + 1)
            if minute not in minute_counts
        ] if minute_counts else []
        gaps = [
            (current - previous) / NANOS_PER_SECOND
            for previous, current in zip(listing_timestamps, listing_timestamps[1:])
        ]
        all_missing_minutes.extend((listing_id, minute) for minute in missing_minutes)
        all_gaps.extend(gaps)
        per_listing[str(listing_id)] = {
            "securityId": security_id,
            "recordCount": len(listing_timestamps),
            "capturedMinutes": len(minute_counts),
            "missingMinutes": len(missing_minutes),
            "maxGapSeconds": round(max(gaps), 3) if gaps else None,
            "subscribedAssetResolved": listing_id in subscribed_assets,
            "subscribedAssetSource": subscribed_asset_sources.get(listing_id),
        }
    capture_seconds = (timestamps[-1] - timestamps[0]) / NANOS_PER_SECOND if len(timestamps) > 1 else 0.0
    manifest_error_counts = collections.Counter(manifest_errors)
    return (
        {
            "selectedManifests": selected_manifest_count,
            "checksumsPassed": checksums_passed,
            "manifestErrors": sorted(manifest_error_counts),
            "manifestErrorCounts": dict(sorted(manifest_error_counts.items())),
            "recordCount": len(window_records),
            "captureSeconds": round(capture_seconds, 3),
            "listingCount": len(listing_security_ids),
            "listingIds": sorted(listing_security_ids),
            "securityIds": sorted(set(listing_security_ids.values())),
            "collectionIds": sorted(collection_ids),
            "contractMetadataResolved": bool(metadata_keys)
            and set(listing_security_ids).issubset(metadata_listing_ids)
            and len(collection_ids) == 1
            and not any("metadata" in error for error in manifest_errors),
            "perListing": per_listing,
            "capturedMinutes": min((value["capturedMinutes"] for value in per_listing.values()), default=0),
            "missingMinutes": len(all_missing_minutes),
            "maxGapSeconds": round(max(all_gaps), 3) if all_gaps else None,
            "jsonErrors": json_errors,
            "payloadShapes": dict(payload_shapes),
            "eventTypes": dict(event_types),
            "subscribedAssetResolved": bool(listing_security_ids)
            and len(subscribed_assets) == len(listing_security_ids),
            "subscribedAssetSources": {str(key): value for key, value in subscribed_asset_sources.items()},
            "actionableEvents": sum(actionable.values()),
            "actionableEventToIngressLatencyMs": summary(actionable_latencies),
            "actionableIngressOutliers": actionable_outliers,
            "incrementalEventToIngressLatencyMs": summary(incremental_latencies),
            "incrementalIngressOutliers": incremental_outliers,
            "snapshotAgeMs": summary(snapshot_ages),
            "snapshotAgeOutliers": snapshot_age_outliers,
        },
        actionable,
    )


def compile_normalized_inspector(repository: Path, work: Path, collector_jar: Path) -> tuple[Path, str]:
    if not collector_jar.is_file():
        raise AcceptanceError(
            f"Collector JAR is unavailable: {collector_jar}. Build the sandbox image artifact first."
        )
    classes = work / "inspector-classes"
    classes.mkdir()
    classpath = str(collector_jar)
    run([
        "javac",
        "-cp",
        classpath,
        "-d",
        str(classes),
        str(repository / "tools" / "NormalizedDataInspector.java"),
    ], repository)
    return classes, classpath


def normalized_report(
    profile: str,
    region: str,
    bucket: str,
    objects: list[dict[str, Any]],
    start_ns: int,
    end_ns: int,
    repository: Path,
    collector_jar: Path,
    work: Path,
) -> tuple[dict[str, Any], collections.Counter[tuple[int, int]]]:
    normalized_dir = work / "normalized"
    candidates = [item for item in objects if item["Key"].endswith(".zst")]
    for index, item in enumerate(sorted(candidates, key=lambda value: value["Key"])):
        download_object(profile, region, bucket, item["Key"], normalized_dir / f"{index}.zst")

    classes, classpath = compile_normalized_inspector(repository, work, collector_jar)
    result = run([
        "java",
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "-cp",
        f"{classes}:{classpath}",
        "NormalizedDataInspector",
        str(normalized_dir),
    ])
    records: list[dict[str, Any]] = []
    invalid_files = 0
    for line in result.stdout.splitlines():
        entry = json.loads(line)
        if entry["kind"] == "record" and start_ns <= int(entry["receiveTimestamp"]) < end_ns:
            records.append(entry)
        elif entry["kind"] == "fileError":
            invalid_files += 1

    timestamps = collections.Counter(
        (int(record["securityId"]), int(record["eventTimestamp"])) for record in records
    )
    exchange_ids = {int(record["exchangeId"]) for record in records}
    security_ids = {int(record["securityId"]) for record in records}
    exchanges_by_security: dict[int, set[int]] = collections.defaultdict(set)
    for record in records:
        exchanges_by_security[int(record["securityId"])].add(int(record["exchangeId"]))
    sequence_gaps_by_security: dict[int, int] = collections.Counter()
    last_sequence_by_security: dict[int, int] = {}
    for record in records:
        sequence = int(record["sequence"])
        security_id = int(record["securityId"])
        if sequence == 0:
            continue
        previous = last_sequence_by_security.get(security_id)
        if previous is not None and sequence > previous + 1:
            sequence_gaps_by_security[security_id] += sequence - previous - 1
        if previous is None or sequence > previous:
            last_sequence_by_security[security_id] = sequence
    latencies = [
        (int(record["receiveTimestamp"]) - int(record["eventTimestamp"])) / 1_000_000
        for record in records
        if int(record["receiveTimestamp"]) > 0
    ]
    latency_outliers = [
        {
            "eventTimestamp": int(record["eventTimestamp"]),
            "receiveTimestamp": int(record["receiveTimestamp"]),
            "latencyMs": round(
                (int(record["receiveTimestamp"]) - int(record["eventTimestamp"])) / 1_000_000,
                3,
            ),
            "action": str(record["action"]),
        }
        for record in records
        if int(record["receiveTimestamp"]) > 0
        and int(record["receiveTimestamp"]) - int(record["eventTimestamp"]) > NANOS_PER_SECOND
    ]
    actions = collections.Counter(str(record["action"]) for record in records)
    return (
        {
            "downloadedFiles": len(candidates),
            "invalidFiles": invalid_files,
            "messageCount": len(records),
            "actions": dict(actions),
            "identifiersPresent": bool(exchange_ids and security_ids and min(exchange_ids) > 0 and min(security_ids) > 0),
            "identifiersStable": bool(exchanges_by_security)
            and all(len(exchanges) == 1 for exchanges in exchanges_by_security.values()),
            "exchangeIds": sorted(exchange_ids),
            "securityIds": sorted(security_ids),
            "securityCount": len(security_ids),
            "missingReceiveTimestamps": sum(int(record["receiveTimestamp"]) <= 0 for record in records),
            "nonNullSequences": sum(record["nonNullSequence"] for record in records),
            "sequenceGapCount": sum(sequence_gaps_by_security.values()),
            "sequenceGapsBySecurity": {str(key): value for key, value in sequence_gaps_by_security.items()},
            "invalidDepths": sum(record["invalidDepth"] for record in records),
            "minPopulatedBidLevels": min((int(record["populatedBidLevels"]) for record in records), default=0),
            "minPopulatedAskLevels": min((int(record["populatedAskLevels"]) for record in records), default=0),
            "crossedBooks": sum(record["crossedBook"] for record in records),
            "invalidPrices": sum(record["invalidPrice"] for record in records),
            "negativeLatencies": sum(latency < 0 for latency in latencies),
            "eventsOverOneSecond": sum(latency > 1_000 for latency in latencies),
            "latencyOutliers": latency_outliers,
            "eventToReceiveLatencyMs": summary(latencies),
        },
        timestamps,
    )


def runtime_report(profile: str, region: str, context: dict[str, str], start: dt.datetime) -> dict[str, Any]:
    response = aws_json(
        profile,
        region,
        "ecs",
        "describe-services",
        "--cluster",
        context["CollectorClusterName"],
        "--services",
        context["CollectorServiceName"],
    )
    services = response.get("services", [])
    if len(services) != 1:
        raise AcceptanceError("Unable to resolve the ECS collector service")
    service = services[0]
    primary = next((deployment for deployment in service.get("deployments", []) if deployment.get("status") == "PRIMARY"), {})
    running_arns = aws_json(
        profile,
        region,
        "ecs",
        "list-tasks",
        "--cluster",
        context["CollectorClusterName"],
        "--service-name",
        context["CollectorServiceName"],
        "--desired-status",
        "RUNNING",
    ).get("taskArns", [])
    health: list[str] = []
    task_starts: list[str] = []
    current_definition = primary.get("taskDefinition")
    if running_arns:
        tasks = aws_json(
            profile,
            region,
            "ecs",
            "describe-tasks",
            "--cluster",
            context["CollectorClusterName"],
            "--tasks",
            *running_arns,
        ).get("tasks", [])
        health = [task.get("healthStatus", "UNKNOWN") for task in tasks]
        task_starts = [iso_time(parse_timestamp(task["startedAt"])) for task in tasks if task.get("startedAt")]

    stopped_arns = aws_json(
        profile,
        region,
        "ecs",
        "list-tasks",
        "--cluster",
        context["CollectorClusterName"],
        "--service-name",
        context["CollectorServiceName"],
        "--desired-status",
        "STOPPED",
    ).get("taskArns", [])
    restart_count = 0
    if stopped_arns:
        stopped = aws_json(
            profile,
            region,
            "ecs",
            "describe-tasks",
            "--cluster",
            context["CollectorClusterName"],
            "--tasks",
            *stopped_arns,
        ).get("tasks", [])
        restart_count = sum(
            task.get("taskDefinitionArn") == current_definition
            and task.get("stoppedAt")
            and parse_timestamp(task["stoppedAt"]) >= start
            for task in stopped
        )

    tasks_started_after_window_start = sum(parse_timestamp(value) > start for value in task_starts)

    return {
        "stackStatus": context["StackStatus"],
        "desiredTasks": service.get("desiredCount", 0),
        "runningTasks": service.get("runningCount", 0),
        "pendingTasks": service.get("pendingCount", 0),
        "rolloutState": primary.get("rolloutState", "UNKNOWN"),
        "failedTasks": primary.get("failedTasks", 0),
        "taskHealth": health,
        "taskStartedAt": task_starts,
        "tasksStartedAfterWindowStart": tasks_started_after_window_start,
        "restartsSinceWindowStart": restart_count,
    }


def operations_report(profile: str, region: str, context: dict[str, str]) -> dict[str, Any]:
    retention_days: list[int] = []
    for bucket_key in ("VenueRawBucketName", "NormalizedRawBucketName"):
        lifecycle = aws_json(
            profile,
            region,
            "s3api",
            "get-bucket-lifecycle-configuration",
            "--bucket",
            context[bucket_key],
        )
        enabled_days = [
            int(rule["Expiration"]["Days"])
            for rule in lifecycle.get("Rules", [])
            if rule.get("Status") == "Enabled" and rule.get("Expiration", {}).get("Days") is not None
        ]
        retention_days.append(min(enabled_days) if enabled_days else 0)

    account_id = str(aws_json(profile, region, "sts", "get-caller-identity")["Account"])
    budget = aws_json(
        profile,
        region,
        "budgets",
        "describe-budget",
        "--account-id",
        account_id,
        "--budget-name",
        SANDBOX_BUDGET_NAME,
    )["Budget"]
    notifications = aws_json(
        profile,
        region,
        "budgets",
        "describe-notifications-for-budget",
        "--account-id",
        account_id,
        "--budget-name",
        SANDBOX_BUDGET_NAME,
    ).get("Notifications", [])
    actual_notifications = [
        notification
        for notification in notifications
        if notification.get("NotificationType") == "ACTUAL"
        and notification.get("ThresholdType") == "ABSOLUTE_VALUE"
        and notification.get("ComparisonOperator") == "GREATER_THAN"
    ]
    email_subscriber_thresholds: list[float] = []
    for notification in actual_notifications:
        subscriber_types = aws_json(
            profile,
            region,
            "budgets",
            "describe-subscribers-for-notification",
            "--account-id",
            account_id,
            "--budget-name",
            SANDBOX_BUDGET_NAME,
            "--notification",
            ",".join([
                f"NotificationType={notification['NotificationType']}",
                f"ComparisonOperator={notification['ComparisonOperator']}",
                f"Threshold={notification['Threshold']}",
                f"ThresholdType={notification['ThresholdType']}",
            ]),
            "--query",
            "Subscribers[].SubscriptionType",
        )
        if "EMAIL" in subscriber_types:
            email_subscriber_thresholds.append(float(notification["Threshold"]))
    return {
        "retentionDays": retention_days,
        "budgetLimitUsd": float(budget["BudgetLimit"]["Amount"]),
        "budgetActualUsd": float(budget["CalculatedSpend"]["ActualSpend"]["Amount"]),
        "budgetTimeUnit": budget["TimeUnit"],
        "budgetNotificationThresholdsUsd": sorted(float(item["Threshold"]) for item in actual_notifications),
        "emailSubscriberThresholdsUsd": sorted(email_subscriber_thresholds),
    }


def parity_report(
    raw: collections.Counter[tuple[int, int]],
    normalized: collections.Counter[tuple[int, int]],
) -> dict[str, Any]:
    missing = raw - normalized
    extra = normalized - raw
    matched = raw & normalized
    missing_samples = [
        {"securityId": security_id, "eventTimestamp": event_timestamp, "count": count}
        for (security_id, event_timestamp), count in sorted(missing.items())[:20]
    ]
    extra_samples = [
        {"securityId": security_id, "eventTimestamp": event_timestamp, "count": count}
        for (security_id, event_timestamp), count in sorted(extra.items())[:20]
    ]
    return {
        "rawActionableEvents": sum(raw.values()),
        "normalizedEvents": sum(normalized.values()),
        "matchedEvents": sum(matched.values()),
        "missingNormalizedEvents": sum(missing.values()),
        "extraNormalizedEvents": sum(extra.values()),
        "missingNormalizedSamples": missing_samples,
        "extraNormalizedSamples": extra_samples,
    }


def evaluate(
    raw: dict[str, Any],
    normalized: dict[str, Any],
    parity: dict[str, Any],
    runtime: dict[str, Any],
    operations: dict[str, Any],
    complete: bool,
    max_gap_seconds: float,
) -> list[dict[str, Any]]:
    checks = [
        ("window.complete", complete, "Requested window and settlement delay have elapsed"),
        ("raw.records", raw["recordCount"] > 0, "Raw records were captured"),
        (
            "raw.listings",
            raw["listingIds"] == raw["configuredListingIds"],
            "Every configured listing is present and no unexpected listing was captured",
        ),
        ("raw.manifests", not raw["manifestErrors"], "Raw manifests, checksums, and record metadata match"),
        ("raw.metadata", raw["contractMetadataResolved"], "Collection contract metadata is present and valid"),
        ("raw.json", raw["jsonErrors"] == 0, "All non-control payloads decode as JSON"),
        ("raw.asset", raw["subscribedAssetResolved"], "The subscribed Polymarket outcome asset is resolved"),
        ("raw.minutes", raw["missingMinutes"] == 0, "No minute is missing between first and last record"),
        (
            "raw.gaps",
            raw["maxGapSeconds"] is not None and raw["maxGapSeconds"] <= max_gap_seconds,
            f"Maximum raw receive gap is at most {max_gap_seconds:g} seconds",
        ),
        ("normalized.records", normalized["messageCount"] > 0, "Normalized records were emitted"),
        ("normalized.files", normalized["invalidFiles"] == 0, "Every normalized file is valid zstd/SBE"),
        (
            "normalized.identifiers",
            normalized["identifiersPresent"]
            and normalized["identifiersStable"]
            and normalized["securityIds"] == raw["securityIds"],
            "Identifiers are present, stable per listing, and match the raw collection",
        ),
        ("normalized.receiveTime", normalized["missingReceiveTimestamps"] == 0, "Receive timestamps are present"),
        ("normalized.sequence", normalized["nonNullSequences"] == 0, "Polymarket sequence numbers remain null"),
        ("normalized.depth", normalized["invalidDepths"] == 0, "Depth and all MBP-10 levels decode correctly"),
        ("normalized.books", normalized["crossedBooks"] == 0 and normalized["invalidPrices"] == 0, "Books and prices are valid"),
        ("latency.nonNegative", normalized["negativeLatencies"] == 0, "No event-to-receive latency is negative"),
        (
            "latency.p95",
            raw["incrementalEventToIngressLatencyMs"]["p95"] is not None
            and raw["incrementalEventToIngressLatencyMs"]["p95"] <= 500,
            "Incremental event-to-ingress p95 is at most 500 ms",
        ),
        (
            "latency.outliers",
            not raw["incrementalIngressOutliers"],
            "No incremental event exceeds one second; subscription snapshot age is reported separately",
        ),
        ("parity.missing", parity["missingNormalizedEvents"] == 0, "Every actionable raw event has normalized output"),
        ("parity.extra", parity["extraNormalizedEvents"] == 0, "Every normalized event has a raw source event"),
        ("runtime.stack", runtime["stackStatus"] == "UPDATE_COMPLETE", "CloudFormation stack is stable"),
        (
            "runtime.tasks",
            runtime["desiredTasks"] == runtime["runningTasks"] == 1 and runtime["pendingTasks"] == 0,
            "Exactly one desired collector task is running",
        ),
        ("runtime.health", runtime["taskHealth"] == ["HEALTHY"], "The collector task is healthy"),
        ("runtime.rollout", runtime["rolloutState"] == "COMPLETED" and runtime["failedTasks"] == 0, "The active ECS rollout completed without failures"),
        (
            "runtime.restarts",
            runtime["restartsSinceWindowStart"] == 0 and runtime["tasksStartedAfterWindowStart"] == 0,
            "The current task revision ran for the entire window without replacement",
        ),
        (
            "operations.retention",
            operations["retentionDays"] == [REQUIRED_RETENTION_DAYS, REQUIRED_RETENTION_DAYS],
            "Both raw buckets expire objects after 30 days",
        ),
        (
            "operations.budget",
            operations["budgetTimeUnit"] == "MONTHLY"
            and operations["budgetLimitUsd"] == 100.0
            and operations["budgetActualUsd"] <= operations["budgetLimitUsd"],
            "The monthly sandbox budget is $100 and actual spend remains below it",
        ),
        (
            "operations.alerts",
            set(operations["budgetNotificationThresholdsUsd"]) == REQUIRED_BUDGET_THRESHOLDS
            and set(operations["emailSubscriberThresholdsUsd"]) == REQUIRED_BUDGET_THRESHOLDS,
            "The $10/$25/$50/$100 actual-spend alerts have an email subscriber",
        ),
    ]
    return [{"name": name, "passed": passed, "description": description} for name, passed, description in checks]


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# Collector acceptance report",
        "",
        f"Status: **{report['status']}**",
        "",
        "## Window",
        "",
        f"- Start: `{report['window']['start']}`",
        f"- End: `{report['window']['end']}`",
        f"- Complete: `{str(report['window']['complete']).lower()}`",
        "",
        "## Acceptance checks",
        "",
        "| Check | Result | Requirement |",
        "|---|---:|---|",
    ]
    lines.extend(
        f"| `{check['name']}` | {'PASS' if check['passed'] else 'FAIL'} | {check['description']} |"
        for check in report["checks"]
    )
    raw = report["raw"]
    normalized = report["normalized"]
    parity = report["parity"]
    runtime = report["runtime"]
    operations = report["operations"]
    lines.extend([
        "",
        "## Evidence",
        "",
        f"- Raw records: `{raw['recordCount']}` across `{raw['capturedMinutes']}` captured minutes",
        f"- Listings: `{raw['listingCount']}`; per-listing evidence: `{json.dumps(raw['perListing'], sort_keys=True)}`",
        f"- Collection metadata IDs: `{json.dumps(raw['collectionIds'])}`",
        f"- Raw maximum gap: `{raw['maxGapSeconds']}` seconds",
        f"- Payload shapes: `{json.dumps(raw['payloadShapes'], sort_keys=True)}`",
        f"- Event types: `{json.dumps(raw['eventTypes'], sort_keys=True)}`",
        f"- Normalized messages: `{normalized['messageCount']}`",
        f"- Normalized security IDs: `{normalized['securityIds']}`",
        f"- Sequence gaps by security: `{json.dumps(normalized['sequenceGapsBySecurity'], sort_keys=True)}`",
        f"- Invalid depths: `{normalized['invalidDepths']}`; minimum populated bid/ask levels: `{normalized['minPopulatedBidLevels']}/{normalized['minPopulatedAskLevels']}`",
        f"- Raw/normalized matched events: `{parity['matchedEvents']}`",
        f"- Missing/extra normalized events: `{parity['missingNormalizedEvents']}/{parity['extraNormalizedEvents']}`",
        f"- Event-to-receive latency (ms): `{json.dumps(normalized['eventToReceiveLatencyMs'], sort_keys=True)}`",
        f"- Negative / over-one-second latencies: `{normalized['negativeLatencies']}/{normalized['eventsOverOneSecond']}`",
        f"- Raw ingress latency (ms): `{json.dumps(raw['actionableEventToIngressLatencyMs'], sort_keys=True)}`",
        f"- Raw ingress outliers: `{json.dumps(raw['actionableIngressOutliers'], sort_keys=True)}`",
        f"- Incremental event-to-ingress latency (ms): `{json.dumps(raw['incrementalEventToIngressLatencyMs'], sort_keys=True)}`",
        f"- Incremental ingress outliers: `{json.dumps(raw['incrementalIngressOutliers'], sort_keys=True)}`",
        f"- Subscription snapshot age (ms): `{json.dumps(raw['snapshotAgeMs'], sort_keys=True)}`",
        f"- Snapshot-age outliers: `{json.dumps(raw['snapshotAgeOutliers'], sort_keys=True)}`",
        f"- Normalized latency outliers: `{json.dumps(normalized['latencyOutliers'], sort_keys=True)}`",
        f"- ECS desired/running/pending: `{runtime['desiredTasks']}/{runtime['runningTasks']}/{runtime['pendingTasks']}`",
        f"- Current task start(s): `{json.dumps(runtime['taskStartedAt'])}`",
        f"- Restarts since window start: `{runtime['restartsSinceWindowStart']}`",
        f"- Raw bucket retention days: `{operations['retentionDays']}`",
        f"- Monthly budget actual / limit (USD): `{operations['budgetActualUsd']:.3f}/{operations['budgetLimitUsd']:.2f}`",
        f"- Budget alert thresholds (USD): `{operations['budgetNotificationThresholdsUsd']}`; email-subscribed thresholds: `{operations['emailSubscriberThresholdsUsd']}`",
        "",
    ])
    return "\n".join(lines)


def verify_requirements() -> None:
    missing = [name for name in ("aws", "zstd", "javac", "java") if shutil.which(name) is None]
    if missing:
        raise AcceptanceError(f"Missing required commands: {', '.join(missing)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="gnome-sandbox")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--stack", default="GnomePolymarketSandbox")
    parser.add_argument("--start", required=True, type=parse_timestamp)
    parser.add_argument("--duration-minutes", type=int, default=60)
    parser.add_argument("--settle-seconds", type=int, default=120)
    parser.add_argument("--max-gap-seconds", type=float, default=30.0)
    parser.add_argument("--context-lookback-minutes", type=int, default=60)
    parser.add_argument("--subscribed-asset-id")
    parser.add_argument(
        "--subscribed-asset",
        action="append",
        default=[],
        type=parse_subscribed_asset,
        metavar="LISTING_ID=ASSET_ID",
        help="Override an outcome asset for one listing; repeat for multiple listings",
    )
    parser.add_argument("--allow-incomplete", action="store_true")
    parser.add_argument("--collector-jar", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()

    if (
        args.duration_minutes <= 0
        or args.settle_seconds < 0
        or args.max_gap_seconds <= 0
        or args.context_lookback_minutes <= 0
    ):
        parser.error("Duration, gap, and context lookback must be positive; settlement delay cannot be negative")
    verify_requirements()
    repository = Path(__file__).resolve().parents[1]
    collector_jar = args.collector_jar or repository / "cdk" / "docker" / "sandbox-collector" / "app.jar"
    collector_jar = collector_jar.resolve()
    requested_end = args.start + dt.timedelta(minutes=args.duration_minutes)
    now = dt.datetime.now(dt.timezone.utc)
    complete = now >= requested_end + dt.timedelta(seconds=args.settle_seconds)
    effective_end = requested_end if complete else now
    if effective_end <= args.start:
        raise AcceptanceError("The requested window has not started")
    if not complete and not args.allow_incomplete:
        eligible_at = requested_end + dt.timedelta(seconds=args.settle_seconds)
        raise AcceptanceError(f"Window has not settled; rerun at or after {iso_time(eligible_at)}")

    output_dir = args.output_dir or repository / "build" / "collector-acceptance" / args.start.strftime("%Y%m%dT%H%M%SZ")
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gnome-collector-acceptance-") as temporary:
        work = Path(temporary)
        context = stack_context(args.profile, args.region, args.stack)
        slack = dt.timedelta(minutes=5, seconds=args.settle_seconds)
        raw_context_slack = dt.timedelta(minutes=args.context_lookback_minutes, seconds=args.settle_seconds)
        raw_objects = [
            item
            for item in list_objects(args.profile, args.region, context["VenueRawBucketName"])
            if args.start - raw_context_slack <= modified_at(item) <= effective_end + slack
        ]
        normalized_objects = [
            item
            for item in list_objects(args.profile, args.region, context["NormalizedRawBucketName"])
            if args.start - slack <= modified_at(item) <= effective_end + slack
        ]
        _OBJECT_CACHE.clear()
        prefetch_objects(
            args.profile,
            args.region,
            context["VenueRawBucketName"],
            raw_objects,
            work / "cache" / "raw",
        )
        prefetch_objects(
            args.profile,
            args.region,
            context["NormalizedRawBucketName"],
            normalized_objects,
            work / "cache" / "normalized",
        )
        start_ns = int(args.start.timestamp() * NANOS_PER_SECOND)
        end_ns = int(effective_end.timestamp() * NANOS_PER_SECOND)
        raw, raw_events = raw_report(
            args.profile,
            args.region,
            context["VenueRawBucketName"],
            raw_objects,
            start_ns,
            end_ns,
            args.context_lookback_minutes * NANOS_PER_MINUTE,
            dict(args.subscribed_asset),
            args.subscribed_asset_id,
            work,
        )
        raw["configuredListingIds"] = sorted(
            int(value.strip())
            for value in context["ConfiguredListingIds"].split(",")
            if value.strip()
        )
        normalized, normalized_events = normalized_report(
            args.profile,
            args.region,
            context["NormalizedRawBucketName"],
            normalized_objects,
            start_ns,
            end_ns,
            repository,
            collector_jar,
            work,
        )
        runtime = runtime_report(args.profile, args.region, context, args.start)
        operations = operations_report(args.profile, args.region, context)
        parity = parity_report(raw_events, normalized_events)
        checks = evaluate(raw, normalized, parity, runtime, operations, complete, args.max_gap_seconds)

    failed = [check for check in checks if not check["passed"] and (check["name"] != "window.complete" or not args.allow_incomplete)]
    status = "FAIL" if failed else ("PASS" if complete else "INCOMPLETE")
    report = {
        "status": status,
        "generatedAt": iso_time(now),
        "window": {
            "start": iso_time(args.start),
            "end": iso_time(requested_end),
            "evaluatedThrough": iso_time(effective_end),
            "complete": complete,
            "durationMinutes": args.duration_minutes,
            "settleSeconds": args.settle_seconds,
        },
        "checks": checks,
        "raw": raw,
        "normalized": normalized,
        "parity": parity,
        "runtime": runtime,
        "operations": operations,
    }
    json_path = output_dir / "report.json"
    markdown_path = output_dir / "report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    markdown_path.write_text(markdown_report(report))
    print(f"Collector acceptance: {status}")
    print(f"JSON report: {json_path}")
    print(f"Markdown report: {markdown_path}")
    if failed:
        print("Failed checks: " + ", ".join(check["name"] for check in failed))
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AcceptanceError as error:
        print(f"collector_acceptance: {error}", file=sys.stderr)
        sys.exit(2)
