package group.gnometrading.collection.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.SourceLimits;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class PlacementTest {

    private static final Partition US = new Partition("us-east-1", "VENUE", Priority.RESEARCH, "standard");
    private static final Partition TOKYO = new Partition("ap-northeast-1", "VENUE", Priority.RESEARCH, "standard");

    private static final SourceLimits KALSHI = new SourceLimits("KALSHI", 250, SourceLimits.Scope.ACCOUNT, 180, true);
    private static final SourceLimits HYPERLIQUID =
            new SourceLimits("HYPERLIQUID", 50, SourceLimits.Scope.IP, 8, false);
    private static final SourceLimits PM_US = new SourceLimits("POLYMARKET_US", 100, SourceLimits.Scope.IP, 0, true);

    private final AtomicInteger ids = new AtomicInteger();
    private final List<ShardLoad> loads = new ArrayList<>();
    private final Map<Partition, Integer> maxShards = new HashMap<>();
    private final Map<String, List<Account>> accounts = new HashMap<>(Map.of(
            "KALSHI", List.of(new Account("KALSHI", "md-1", 0, true)),
            "POLYMARKET_US", List.of(new Account("POLYMARKET_US", "md-1", 0, true))));

    private Placement placement(final int maxStreamsPerShard) {
        return new Placement(
                Map.of("KALSHI", KALSHI, "HYPERLIQUID", HYPERLIQUID, "POLYMARKET_US", PM_US),
                accounts,
                maxStreamsPerShard,
                maxShards,
                partition -> "shard-" + ids.incrementAndGet());
    }

    private static List<Placement.Request> requests(final String source, final Partition partition, final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new Placement.Request(i, source, partition))
                .collect(Collectors.toList());
    }

    private static Map<String, Long> perShard(final Placement.Result result) {
        return result.assigned().stream()
                .collect(Collectors.groupingBy(Placement.Assigned::shardId, Collectors.counting()));
    }

    @Test
    void packsOntoOneShardUntilItIsFullThenOpensAnother() {
        Placement.Result result = placement(500).place(loads, requests("KALSHI", US, 600));

        assertEquals(600, result.assigned().size());
        assertEquals(Map.of("shard-1", 500L, "shard-2", 100L), perShard(result));
        assertEquals(2, result.created().size());
        assertEquals("md-1", result.created().get(0).accounts().get("KALSHI"), "bound on its first Kalshi stream");
    }

    @Test
    void anExistingShardWithRoomIsFilledBeforeANewOne() {
        ShardLoad existing = new ShardLoad("old", US, true, Map.of("KALSHI", "md-1"));
        for (int i = 0; i < 10; i++) {
            existing.add("KALSHI");
        }
        loads.add(existing);

        Placement.Result result = placement(500).place(loads, requests("KALSHI", US, 5));

        assertEquals(Map.of("old", 5L), perShard(result));
        assertTrue(result.created().isEmpty());
    }

    @Test
    void aShardDrainingOrRollingTakesNothingNew() {
        loads.add(new ShardLoad("draining", US, false, Map.of()));

        Placement.Result result = placement(500).place(loads, requests("KALSHI", US, 1));

        assertEquals("shard-1", result.assigned().get(0).shardId());
    }

    @Test
    void partitionsNeverMix() {
        List<Placement.Request> requests = new ArrayList<>(requests("KALSHI", US, 3));
        requests.addAll(requests("HYPERLIQUID", TOKYO, 3));

        Placement.Result result = placement(500).place(loads, requests);

        assertEquals(2, result.created().size());
        assertEquals(US, result.created().get(0).partition());
        assertEquals(TOKYO, result.created().get(1).partition());
    }

    @Test
    void aPerIpLimitIsCountedPerShardLeavingASpareConnection() {
        // Hyperliquid: 8 connections an IP, 50 markets each, one kept spare for a roll: 7 x 50 on a shard.
        Placement.Result result = placement(1_000).place(loads, requests("HYPERLIQUID", TOKYO, 351));

        assertEquals(Map.of("shard-1", 350L, "shard-2", 1L), perShard(result));
    }

    @Test
    void anAccountsBudgetIsSharedByEveryShardBoundToIt() {
        // 7 connections: two full shards of 500 Kalshi streams (3 connections each) leave 1, too few for a third.
        accounts.put("KALSHI", List.of(new Account("KALSHI", "md-1", 7, true)));

        Placement.Result result = placement(500).place(loads, requests("KALSHI", US, 1_001));

        assertEquals(1_000, result.assigned().size());
        assertEquals(List.of(new Placement.Unplaced(1_000, Placement.Unplaced.Reason.BUDGET)), result.unplaced());
    }

    @Test
    void aNewShardBindsTheActiveAccountWithTheMostRoom() {
        accounts.put(
                "KALSHI",
                List.of(
                        new Account("KALSHI", "md-1", 7, true),
                        new Account("KALSHI", "md-2", 0, true),
                        new Account("KALSHI", "md-3", 1_000, false)));

        Placement.Result result = placement(500).place(loads, requests("KALSHI", US, 1_500));

        assertEquals(1_500, result.assigned().size());
        assertEquals(
                List.of("md-2", "md-2", "md-2"),
                result.created().stream().map(s -> s.accounts().get("KALSHI")).toList(),
                "md-2 has 180 to md-1's 7; md-3 is retired");
    }

    @Test
    void kalshiAtScaleStaysWithinOneAccountsBudget() {
        // The exit test's case: 10,000 Kalshi streams, one account of 180 connections, 500 a shard.
        Placement.Result result = placement(500).place(loads, requests("KALSHI", US, 10_000));

        int connections = loads.stream()
                .mapToInt(shard -> KALSHI.connectionsFor(shard.streams("KALSHI")))
                .sum();
        assertTrue(connections <= 180, connections + " connections");
        assertEquals(10_000, result.assigned().size(), "60 shards x 3 connections fits 30,000");
    }

    @Test
    void aSignedSourceWithNoActiveAccountIsLeftUnplaced() {
        accounts.put("POLYMARKET_US", List.of(new Account("POLYMARKET_US", "md-1", 0, false)));

        Placement.Result result = placement(500).place(loads, requests("POLYMARKET_US", US, 2));

        assertTrue(result.assigned().isEmpty());
        assertEquals(2, result.unplaced().size());
        assertTrue(result.unplaced().stream().allMatch(u -> u.reason() == Placement.Unplaced.Reason.BUDGET));
    }

    @Test
    void maxShardsStopsNewShardsAndLeavesTheRestUnplacedForCapacity() {
        maxShards.put(US, 1);

        Placement.Result result = placement(500).place(loads, requests("KALSHI", US, 502));

        assertEquals(Map.of("shard-1", 500L), perShard(result));
        assertEquals(
                List.of(
                        new Placement.Unplaced(500, Placement.Unplaced.Reason.CAPACITY),
                        new Placement.Unplaced(501, Placement.Unplaced.Reason.CAPACITY)),
                result.unplaced());
    }

    @Test
    void maxShardsCountsDrainingShardsAndCapsOnlyItsOwnPartition() {
        maxShards.put(US, 1);
        loads.add(new ShardLoad("draining", US, false, Map.of()));
        List<Placement.Request> requests = new ArrayList<>(requests("KALSHI", US, 1));
        requests.add(new Placement.Request(9, "HYPERLIQUID", TOKYO));

        Placement.Result result = placement(500).place(loads, requests);

        assertEquals(List.of(new Placement.Unplaced(0, Placement.Unplaced.Reason.CAPACITY)), result.unplaced());
        assertEquals(TOKYO, result.created().get(0).partition(), "no cap there");
    }

    @Test
    void aStreamLeavingADrainingShardMayTakeThePartitionOneOverMaxShards() {
        maxShards.put(US, 1);
        loads.add(new ShardLoad("rolling", US, false, Map.of()));
        List<Placement.Request> moving =
                List.of(new Placement.Request(1, "KALSHI", US, true), new Placement.Request(2, "KALSHI", US, true));

        Placement.Result result = placement(1).place(loads, moving);

        assertEquals(Map.of("shard-1", 1L), perShard(result), "one over, never two");
        assertEquals(List.of(new Placement.Unplaced(2, Placement.Unplaced.Reason.CAPACITY)), result.unplaced());
    }

    @Test
    void aMaxShardsOfZeroPlacesNothing() {
        maxShards.put(US, 0);

        Placement.Result result = placement(500).place(loads, requests("KALSHI", US, 1));

        assertEquals(
                Placement.Unplaced.Reason.CAPACITY, result.unplaced().get(0).reason());
        assertTrue(result.created().isEmpty());
    }

    @Test
    void anUnknownSourceIsLeftUnplaced() {
        Placement.Result result = placement(500).place(loads, requests("LIGHTER", TOKYO, 1));

        assertEquals(Placement.Unplaced.Reason.SOURCE, result.unplaced().get(0).reason());
    }
}
