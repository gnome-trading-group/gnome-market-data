package group.gnometrading.collection.aws;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.Target;
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
                "1.23.0",
                ShardRecord.State.ACTIVE,
                100,
                0);

        Map<String, AttributeValue> item = DynamoStore.fromShard(shard);

        // gnome-orchestrator DynamoAssignment: big-endian int32s, one binary attribute per list.
        assertArrayEquals(
                new byte[] {0, 0, 0, 1, 0, 0, 1, 0, 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff},
                item.get("streams").b().asByteArray());
        assertEquals("4", item.get("generation").n());
        assertEquals("md-2", item.get("accounts").m().get("KALSHI").s());
        assertEquals(shard, DynamoStore.toShard(item));
    }

    @Test
    void anEmptyShardRoundTrips() {
        ShardRecord shard = new ShardRecord(
                "use1-2", US, 1, List.of(), List.of(), Map.of(), Map.of(), "1.23.0", ShardRecord.State.DRAINING, 5, 9);

        assertEquals(shard, DynamoStore.toShard(DynamoStore.fromShard(shard)));
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
