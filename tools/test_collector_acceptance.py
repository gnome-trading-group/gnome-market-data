import collections
import datetime as dt
import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import collector_acceptance as acceptance
import raw_replay


class CollectorAcceptanceTest(unittest.TestCase):

    def tearDown(self):
        acceptance._OBJECT_CACHE.clear()

    def test_list_objects_reads_every_s3_page(self):
        responses = [
            {"Contents": [{"Key": "first"}], "NextContinuationToken": "next-page"},
            {"Contents": [{"Key": "second"}]},
        ]

        with patch.object(acceptance, "aws_json", side_effect=responses) as mocked:
            objects = acceptance.list_objects("profile", "region", "bucket")

        self.assertEqual([{"Key": "first"}, {"Key": "second"}], objects)
        self.assertEqual(
            (
                "profile",
                "region",
                "s3api",
                "list-objects-v2",
                "--bucket",
                "bucket",
                "--continuation-token",
                "next-page",
            ),
            mocked.call_args_list[1].args,
        )

    @staticmethod
    def passing_evaluation_inputs():
        raw = {
            "recordCount": 1,
            "manifestErrors": [],
            "contractMetadataResolved": True,
            "listingIds": [532],
            "configuredListingIds": [532],
            "jsonErrors": 0,
            "subscribedAssetResolved": True,
            "missingMinutes": 0,
            "maxGapSeconds": 10.0,
            "incrementalEventToIngressLatencyMs": {"p95": 100.0},
            "incrementalIngressOutliers": [],
        }
        normalized = {
            "messageCount": 1,
            "invalidFiles": 0,
            "identifiersPresent": True,
            "identifiersStable": True,
            "securityIds": [499],
            "missingReceiveTimestamps": 0,
            "nonNullSequences": 0,
            "invalidDepths": 0,
            "crossedBooks": 0,
            "invalidPrices": 0,
            "negativeLatencies": 0,
            "eventsOverOneSecond": 0,
            "eventToReceiveLatencyMs": {"p95": 100.0},
        }
        raw["securityIds"] = [499]
        parity = {"missingNormalizedEvents": 0, "extraNormalizedEvents": 0}
        runtime = {
            "stackStatus": "UPDATE_COMPLETE",
            "desiredTasks": 1,
            "runningTasks": 1,
            "pendingTasks": 0,
            "taskHealth": ["HEALTHY"],
            "rolloutState": "COMPLETED",
            "failedTasks": 0,
            "tasksStartedAfterWindowStart": 0,
            "restartsSinceWindowStart": 0,
        }
        operations = {
            "retentionDays": [30, 30],
            "budgetLimitUsd": 100.0,
            "budgetActualUsd": 1.0,
            "budgetTimeUnit": "MONTHLY",
            "budgetNotificationThresholdsUsd": [10.0, 25.0, 50.0, 100.0],
            "emailSubscriberThresholdsUsd": [10.0, 25.0, 50.0, 100.0],
        }
        return raw, normalized, parity, runtime, operations

    def test_iter_raw_records_decodes_header_and_records(self):
        payloads = [(100, b"PONG"), (200, b'{"event_type":"book"}')]
        data = acceptance.RAW_HEADER.pack(b"GNOMERAW", 1, 2501)
        for timestamp, payload in payloads:
            data += acceptance.RAW_RECORD_HEADER.pack(timestamp, len(payload)) + payload

        records = list(acceptance.iter_raw_records(data))

        self.assertEqual([(100, b"PONG", 2501), (200, b'{"event_type":"book"}', 2501)], records)

    def test_iter_raw_records_rejects_truncated_payload(self):
        data = (
            acceptance.RAW_HEADER.pack(b"GNOMERAW", 1, 2501)
            + acceptance.RAW_RECORD_HEADER.pack(100, 10)
            + b"short"
        )

        with self.assertRaises(acceptance.AcceptanceError):
            list(acceptance.iter_raw_records(data))

    def test_raw_report_keeps_concurrent_listing_parity_separate(self):
        start_ns = 1_000_000_000_000
        event_ms = start_ns // 1_000_000 + 1
        receive_ns = start_ns + 2_000_000
        metadata_key = "v1/collections/run-1/contract-metadata.json"
        stored: dict[str, bytes] = {}
        objects: list[dict[str, str]] = []
        for index, (listing_id, security_id, asset_id) in enumerate(
            ((532, 499, "token-a"), (533, 500, "token-b"))
        ):
            payload = json.dumps({
                "event_type": "book",
                "asset_id": asset_id,
                "timestamp": str(event_ms),
            }).encode()
            framed = (
                acceptance.RAW_HEADER.pack(b"GNOMERAW", 1, listing_id)
                + acceptance.RAW_RECORD_HEADER.pack(receive_ns + index, len(payload))
                + payload
            )
            data_key = f"listing-{listing_id}.raw.zst"
            manifest_key = f"listing-{listing_id}.manifest.json"
            stored[data_key] = framed
            stored[manifest_key] = json.dumps({
                "listingId": listing_id,
                "securityId": security_id,
                "collectionId": "run-1",
                "contractMetadataKey": metadata_key,
                "firstReceiveTimestampNanos": receive_ns + index,
                "lastReceiveTimestampNanos": receive_ns + index,
                "messageCount": 1,
                "sha256": hashlib.sha256(framed).hexdigest(),
                "dataKey": data_key,
            }).encode()
            objects.append({"Key": manifest_key})
        stored[metadata_key] = json.dumps({
            "collectionId": "run-1",
            "listings": [
                {"listingId": 532, "exchangeSecurityId": "condition:token-a"},
                {"listingId": 533, "exchangeSecurityId": "condition:token-b"},
            ],
        }).encode()
        old_payload = b"PONG"
        old_receive_ns = start_ns - 1
        old_framed = (
            acceptance.RAW_HEADER.pack(b"GNOMERAW", 1, 532)
            + acceptance.RAW_RECORD_HEADER.pack(old_receive_ns, len(old_payload))
            + old_payload
        )
        stored["old.raw.zst"] = old_framed
        stored["old.manifest.json"] = json.dumps({
            "listingId": 532,
            "securityId": 499,
            "firstReceiveTimestampNanos": old_receive_ns,
            "lastReceiveTimestampNanos": old_receive_ns,
            "messageCount": 1,
            "sha256": hashlib.sha256(old_framed).hexdigest(),
            "dataKey": "old.raw.zst",
        }).encode()
        objects.append({"Key": "old.manifest.json"})

        def fake_download(_profile, _region, _bucket, key, destination):
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(stored[key])

        with tempfile.TemporaryDirectory() as temporary, patch.object(
            acceptance, "download_object", side_effect=fake_download
        ), patch.object(acceptance, "decompress_zstd", side_effect=lambda path: path.read_bytes()):
            report, actionable = acceptance.raw_report(
                "profile",
                "region",
                "bucket",
                objects,
                start_ns,
                start_ns + acceptance.NANOS_PER_SECOND,
                acceptance.NANOS_PER_MINUTE,
                {},
                None,
                Path(temporary),
            )

        self.assertEqual({(499, event_ms * 1_000_000): 1, (500, event_ms * 1_000_000): 1}, actionable)
        self.assertEqual([532, 533], report["listingIds"])
        self.assertEqual([499, 500], report["securityIds"])
        self.assertTrue(report["contractMetadataResolved"])
        self.assertTrue(report["subscribedAssetResolved"])
        self.assertEqual([], report["manifestErrors"])

    def test_parity_report_preserves_duplicate_event_counts(self):
        raw = collections.Counter({(499, 100): 2, (499, 200): 1})
        normalized = collections.Counter({(499, 100): 1, (499, 200): 1, (500, 300): 1})

        report = acceptance.parity_report(raw, normalized)

        self.assertEqual(2, report["matchedEvents"])
        self.assertEqual(1, report["missingNormalizedEvents"])
        self.assertEqual(1, report["extraNormalizedEvents"])
        self.assertEqual(
            [{"securityId": 499, "eventTimestamp": 100, "count": 1}],
            report["missingNormalizedSamples"],
        )
        self.assertEqual(
            [{"securityId": 500, "eventTimestamp": 300, "count": 1}],
            report["extraNormalizedSamples"],
        )

    def test_parse_timestamp_requires_timezone_and_normalizes_to_utc(self):
        parsed = acceptance.parse_timestamp("2026-08-05T15:42:33-07:00")

        self.assertEqual(dt.datetime(2026, 8, 5, 22, 42, 33, tzinfo=dt.timezone.utc), parsed)

        with self.assertRaises(Exception):
            acceptance.parse_timestamp("2026-08-05T15:42:33")

    def test_parse_subscribed_asset_requires_listing_mapping(self):
        self.assertEqual((532, "token-id"), acceptance.parse_subscribed_asset("532=token-id"))

        with self.assertRaises(Exception):
            acceptance.parse_subscribed_asset("token-id")

    def test_replay_fixture_build_is_deterministic(self):
        fixture = Path(__file__).parent / "fixtures" / "polymarket-replay.json"
        expected = json.loads(fixture.read_text())
        with tempfile.TemporaryDirectory() as temporary:
            capture = Path(temporary) / "fixture.raw.zst"
            raw_replay.build_fixture(fixture, capture)

            inspected = raw_replay.inspect_capture(capture)

        self.assertEqual(expected["expectedRawSha256"], inspected["sequenceSha256"])
        self.assertEqual(len(expected["records"]), inspected["recordCount"])

    def test_evaluate_accepts_passing_phase_zero_evidence(self):
        raw, normalized, parity, runtime, operations = self.passing_evaluation_inputs()

        checks = acceptance.evaluate(raw, normalized, parity, runtime, operations, True, 30.0)

        self.assertTrue(all(check["passed"] for check in checks))

    def test_evaluate_rejects_sequence_and_latency_violations(self):
        raw, normalized, parity, runtime, operations = self.passing_evaluation_inputs()
        raw["incrementalEventToIngressLatencyMs"] = {"p95": 500.001}
        raw["incrementalIngressOutliers"] = [{"latencyMs": 1000.001}]
        normalized.update({
            "nonNullSequences": 1,
            "negativeLatencies": 1,
        })

        checks = acceptance.evaluate(raw, normalized, parity, runtime, operations, True, 30.0)
        failed = {check["name"] for check in checks if not check["passed"]}

        self.assertEqual(
            {"normalized.sequence", "latency.nonNegative", "latency.p95", "latency.outliers"},
            failed,
        )

    def test_evaluate_rejects_missing_metadata_depth_and_security(self):
        raw, normalized, parity, runtime, operations = self.passing_evaluation_inputs()
        raw["contractMetadataResolved"] = False
        normalized["invalidDepths"] = 1
        normalized["securityIds"] = [500]

        checks = acceptance.evaluate(raw, normalized, parity, runtime, operations, True, 30.0)
        failed = {check["name"] for check in checks if not check["passed"]}

        self.assertEqual({"raw.metadata", "normalized.identifiers", "normalized.depth"}, failed)

    def test_evaluate_rejects_a_missing_configured_listing(self):
        raw, normalized, parity, runtime, operations = self.passing_evaluation_inputs()
        raw["configuredListingIds"] = [532, 533]

        checks = acceptance.evaluate(raw, normalized, parity, runtime, operations, True, 30.0)
        failed = {check["name"] for check in checks if not check["passed"]}

        self.assertEqual({"raw.listings"}, failed)

    def test_evaluate_rejects_a_replacement_task(self):
        raw, normalized, parity, runtime, operations = self.passing_evaluation_inputs()
        runtime["tasksStartedAfterWindowStart"] = 1

        checks = acceptance.evaluate(raw, normalized, parity, runtime, operations, True, 30.0)
        failed = {check["name"] for check in checks if not check["passed"]}

        self.assertEqual({"runtime.restarts"}, failed)

    def test_evaluate_rejects_an_unresolved_subscribed_asset(self):
        raw, normalized, parity, runtime, operations = self.passing_evaluation_inputs()
        raw["subscribedAssetResolved"] = False

        checks = acceptance.evaluate(raw, normalized, parity, runtime, operations, True, 30.0)
        failed = {check["name"] for check in checks if not check["passed"]}

        self.assertEqual({"raw.asset"}, failed)

    def test_evaluate_rejects_operational_guardrail_violations(self):
        raw, normalized, parity, runtime, operations = self.passing_evaluation_inputs()
        operations.update({
            "retentionDays": [30, 7],
            "budgetActualUsd": 101.0,
            "emailSubscriberThresholdsUsd": [10.0],
        })

        checks = acceptance.evaluate(raw, normalized, parity, runtime, operations, True, 30.0)
        failed = {check["name"] for check in checks if not check["passed"]}

        self.assertEqual(
            {"operations.retention", "operations.budget", "operations.alerts"},
            failed,
        )


if __name__ == "__main__":
    unittest.main()
