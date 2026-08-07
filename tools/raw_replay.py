#!/usr/bin/env python3
"""Build, inspect, and verify deterministic GNOMERAW replay fixtures."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import collector_acceptance as acceptance


def payload_bytes(payload: Any) -> bytes:
    if payload == "PONG":
        return b"PONG"
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def build_fixture(source: Path, destination: Path) -> dict[str, Any]:
    fixture = json.loads(source.read_text())
    listing_id = int(fixture["listingId"])
    base_timestamp = int(fixture["baseReceiveTimestampNanos"])
    records = fixture["records"]
    with tempfile.NamedTemporaryFile(prefix="gnome-replay-", suffix=".raw") as uncompressed:
        uncompressed.write(acceptance.RAW_HEADER.pack(b"GNOMERAW", 1, listing_id))
        for record in records:
            payload = payload_bytes(record["payload"])
            timestamp = base_timestamp + int(record["receiveOffsetNanos"])
            uncompressed.write(acceptance.RAW_RECORD_HEADER.pack(timestamp, len(payload)))
            uncompressed.write(payload)
        uncompressed.flush()
        destination.parent.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            ["zstd", "-q", "-f", uncompressed.name, "-o", str(destination)],
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            raise acceptance.AcceptanceError(result.stderr.strip() or "Unable to compress replay fixture")
    return fixture


def inspect_capture(capture: Path, speed: float = 0.0, emit: bool = False) -> dict[str, Any]:
    records = list(acceptance.iter_raw_records(acceptance.decompress_zstd(capture)))
    digest = hashlib.sha256()
    event_types: dict[str, int] = {}
    previous_timestamp: int | None = None
    for timestamp, payload, listing_id in records:
        digest.update(struct.pack(">qI", timestamp, len(payload)))
        digest.update(payload)
        text = payload.decode("utf-8")
        if text == "PONG":
            event_types["PONG"] = event_types.get("PONG", 0) + 1
            decoded: Any = text
        else:
            decoded = json.loads(text)
            events = decoded if isinstance(decoded, list) else [decoded]
            for event in events:
                event_type = event.get("event_type", "unknown")
                event_types[event_type] = event_types.get(event_type, 0) + 1
        if speed > 0 and previous_timestamp is not None:
            time.sleep(max(0.0, (timestamp - previous_timestamp) / 1_000_000_000 / speed))
        if emit:
            print(json.dumps({
                "receiveTimestampNanos": timestamp,
                "listingId": listing_id,
                "payload": decoded,
            }, separators=(",", ":")))
        previous_timestamp = timestamp
    return {
        "recordCount": len(records),
        "eventTypes": event_types,
        "sequenceSha256": digest.hexdigest(),
        "timestampsMonotonic": all(
            records[index - 1][0] <= records[index][0] for index in range(1, len(records))
        ),
    }


def run_java_harness(
    repository: Path,
    collector_jar: Path,
    capture: Path,
    token_id: str,
    expected_outputs: int,
) -> dict[str, Any]:
    if not collector_jar.is_file():
        raise acceptance.AcceptanceError(f"Collector JAR is unavailable: {collector_jar}")
    with tempfile.TemporaryDirectory(prefix="gnome-replay-harness-") as temporary:
        temporary_path = Path(temporary)
        classes = temporary_path / "classes"
        classes.mkdir()
        uncompressed_capture = temporary_path / "fixture.raw"
        uncompressed_capture.write_bytes(acceptance.decompress_zstd(capture))
        acceptance.run([
            "javac",
            "-cp",
            str(collector_jar),
            "-d",
            str(classes),
            str(repository / "tools" / "PolymarketReplayHarness.java"),
        ])
        result = acceptance.run([
            "java",
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "-cp",
            f"{classes}:{collector_jar}",
            "PolymarketReplayHarness",
            str(uncompressed_capture),
            token_id,
            str(expected_outputs),
        ])
    return json.loads(result.stdout[result.stdout.find("{") :])


def verify_fixture(source: Path, collector_jar: Path) -> dict[str, Any]:
    repository = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="gnome-replay-fixture-") as temporary:
        capture = Path(temporary) / "fixture.raw.zst"
        fixture = build_fixture(source, capture)
        raw = inspect_capture(capture)
        expected = fixture["expected"]
        reader = run_java_harness(
            repository,
            collector_jar,
            capture,
            fixture["tokenId"],
            int(expected["messageCount"]),
        )
    failures: list[str] = []
    for key, value in expected.items():
        if reader.get(key) != value:
            failures.append(f"{key}: expected {value!r}, got {reader.get(key)!r}")
    if raw["recordCount"] != len(fixture["records"]):
        failures.append("raw record count does not match the fixture source")
    if raw["sequenceSha256"] != fixture["expectedRawSha256"]:
        failures.append("raw replay sequence digest does not match the fixture expectation")
    if not raw["timestampsMonotonic"]:
        failures.append("raw receive timestamps are not monotonic")
    return {"passed": not failures, "failures": failures, "raw": raw, "reader": reader}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)

    build = subcommands.add_parser("build", help="Build a compressed GNOMERAW fixture")
    build.add_argument("source", type=Path)
    build.add_argument("destination", type=Path)

    inspect = subcommands.add_parser("inspect", help="Inspect or stream a GNOMERAW capture")
    inspect.add_argument("capture", type=Path)
    inspect.add_argument("--speed", type=float, default=0.0, help="0 is immediate; 1 preserves original timing")
    inspect.add_argument("--emit", action="store_true", help="Emit replay envelopes as newline-delimited JSON")

    verify = subcommands.add_parser("verify", help="Replay a fixture through the Polymarket reader")
    verify.add_argument("source", type=Path)
    verify.add_argument("--collector-jar", type=Path)

    args = parser.parse_args()
    if shutil.which("zstd") is None:
        raise acceptance.AcceptanceError("Missing required command: zstd")
    if args.command == "build":
        build_fixture(args.source, args.destination)
        print(args.destination)
        return 0
    if args.command == "inspect":
        if args.speed < 0:
            parser.error("--speed cannot be negative")
        result = inspect_capture(args.capture, args.speed, args.emit)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    repository = Path(__file__).resolve().parents[1]
    collector_jar = (args.collector_jar or repository / "cdk" / "docker" / "sandbox-collector" / "app.jar").resolve()
    result = verify_fixture(args.source, collector_jar)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except acceptance.AcceptanceError as error:
        print(f"raw_replay: {error}", file=sys.stderr)
        sys.exit(2)
