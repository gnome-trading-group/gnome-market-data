package group.gnometrading.collection.aws;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.Lease;
import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.Target;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class DynamoStoreTest {

    private static final Partition US = new Partition("us-east-1", "VENUE", Priority.RESEARCH, "standard");

    @Test
    void aShardsStreamsArePackedAsTheShardReadsThem() {
        ShardRecord shard = new ShardRecord(
                "use1-1",
                US,
                4,
                List.of(1, 256, Integer.MAX_VALUE),
                List.of(7),
                Map.of("KALSHI", "md-2"),
                Map.of(12, 29_000_000, 13, ShardRecord.WARMING),
                List.of(7, 256),
                "1.23.0",
                ShardRecord.State.ACTIVE,
                100,
                0,
                0);

        Map<String, AttributeValue> item = DynamoStore.fromShard(shard);

        // gnome-orchestrator DynamoAssignment: big-endian int32s, one binary attribute per list.
        assertArrayEquals(
                new byte[] {0, 0, 0, 1, 0, 0, 1, 0, 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff},
                item.get("streams").b().asByteArray());
        assertEquals("4", item.get("generation").n());
        assertEquals("md-2", item.get("accounts").m().get("KALSHI").s());
        assertArrayEquals(
                new byte[] {0, 0, 0, 7, 0, 0, 1, 0}, item.get("publish").b().asByteArray());
        assertEquals(shard, DynamoStore.toShard(item));
    }

    @Test
    void anEmptyShardRoundTrips() {
        ShardRecord shard = new ShardRecord(
                "use1-2",
                US,
                1,
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                List.of(),
                "1.23.0",
                ShardRecord.State.DRAINING,
                5,
                9,
                11);

        assertEquals(shard, DynamoStore.toShard(DynamoStore.fromShard(shard)));
    }

    @Test
    void aLeaseReadsItsPackedStreamsAndEpochSeconds() {
        Map<String, AttributeValue> item = Map.of(
                "lease_id", AttributeValue.fromS("s-1"),
                "streams", AttributeValue.fromB(DynamoStore.pack(List.of(3, 1))),
                "priority", AttributeValue.fromS("PRODUCTION"),
                "tier", AttributeValue.fromS("standard"),
                "mode", AttributeValue.fromS("live"),
                "status", AttributeValue.fromS("ACTIVE"),
                "created_at", AttributeValue.fromN("1700000000"),
                "expires_at", AttributeValue.fromN("1700007200"));

        assertEquals(
                Lease.active(
                        "s-1", Set.of(1, 3), Priority.PRODUCTION, "standard", "live", 1_700_000_000L, 1_700_007_200L),
                DynamoStore.toLease(item));
    }

    @Test
    void aReleasedLeaseReadsWhenAndItsLastSeenSession() {
        Map<String, AttributeValue> item = new HashMap<>(Map.of(
                "lease_id", AttributeValue.fromS("s-1"),
                "streams", AttributeValue.fromB(DynamoStore.pack(List.of(3))),
                "priority", AttributeValue.fromS("RESEARCH"),
                "tier", AttributeValue.fromS("standard"),
                "mode", AttributeValue.fromS("paper"),
                "status", AttributeValue.fromS("RELEASED"),
                "created_at", AttributeValue.fromN("1700000000"),
                "expires_at", AttributeValue.fromN("1700007200")));
        item.put("released_at", AttributeValue.fromN("1700000600"));
        item.put("session_status", AttributeValue.fromS("RUNNING"));
        item.put("session_seen_at", AttributeValue.fromN("1700000500"));

        Lease lease = DynamoStore.toLease(item);

        assertEquals(Lease.Status.RELEASED, lease.status());
        assertEquals(1_700_000_600L, lease.releasedAtSeconds());
        assertEquals("RUNNING", lease.sessionStatus());
        assertEquals(1_700_000_500L, lease.sessionSeenAtSeconds());
    }

    @Test
    void aMalformedLeaseIsSkippedNotFatal() {
        Map<String, AttributeValue> unknownPriority = Map.of(
                "lease_id", AttributeValue.fromS("s-1"),
                "priority", AttributeValue.fromS("URGENT"),
                "status", AttributeValue.fromS("ACTIVE"));
        Map<String, AttributeValue> noId =
                Map.of("priority", AttributeValue.fromS("RESEARCH"), "status", AttributeValue.fromS("ACTIVE"));
        Map<String, AttributeValue> badNumber = Map.of(
                "lease_id", AttributeValue.fromS("s-1"),
                "priority", AttributeValue.fromS("RESEARCH"),
                "status", AttributeValue.fromS("ACTIVE"),
                "expires_at", AttributeValue.fromN("soon"));

        assertNull(DynamoStore.toLease(unknownPriority));
        assertNull(DynamoStore.toLease(noId));
        assertNull(DynamoStore.toLease(badNumber));
    }

    @Test
    void aTargetRoundTripsWithItsOptionalFieldsLeftOut() {
        Target desired = Target.desired(9, "KALSHI", US, Set.of("r1", "r2"), 3, 1_000);
        Target handingOff = desired.handingOff("use1-2", "use1-1").handoffCoveredSince(2_000);

        assertEquals(desired, DynamoStore.toTarget(DynamoStore.fromTarget(desired)));
        assertEquals(handingOff, DynamoStore.toTarget(DynamoStore.fromTarget(handingOff)));

        Target retired = desired.retiring(5_000).retired();
        Map<String, AttributeValue> item = DynamoStore.fromTarget(retired);
        assertFalse(item.containsKey("wanted_by"), "DynamoDB refuses an empty set");
        assertEquals(retired, DynamoStore.toTarget(item));
    }

    @Test
    void onlyAnOpenIntervalIsInTheOpenIndex() {
        Interval open = new Interval(9, 100, 200, "use1-1", "task-1", true, null);
        Interval closed = new Interval(9, 100, 250, "use1-1", "task-1", false, "STALE");

        assertEquals(
                "use1-1", DynamoStore.fromInterval(open).get("open_shard_id").s());
        assertFalse(DynamoStore.fromInterval(closed).containsKey("open_shard_id"));
        assertEquals(open, DynamoStore.toInterval(DynamoStore.fromInterval(open)));
        assertEquals(closed, DynamoStore.toInterval(DynamoStore.fromInterval(closed)));
        assertTrue(DynamoStore.unpack(null).isEmpty());
    }
}
