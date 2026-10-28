package group.gnometrading.collection.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.EventVolume;
import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.RuleSelector;
import group.gnometrading.collection.model.RuleStatus;
import group.gnometrading.collection.model.Settings;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.SourceLimits;
import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.model.StreamState;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.model.TaskStatus;
import group.gnometrading.collection.model.Transition;
import group.gnometrading.collection.ports.Alerts;
import group.gnometrading.collection.ports.Registry;
import group.gnometrading.collection.ports.Services;
import group.gnometrading.collection.ports.Store;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ReconcilerTest {

    private static final long MINUTE = TimeUnit.MINUTES.toNanos(1);
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final long T0 = 20_000L * TimeUnit.DAYS.toNanos(1) + TimeUnit.HOURS.toNanos(6);

    // ----- fakes -----

    private final List<Rule> rules = new ArrayList<>();
    private final Map<Integer, Target> targets = new HashMap<>();
    private final Map<String, ShardRecord> shards = new LinkedHashMap<>();
    private final List<Interval> intervals = new ArrayList<>();
    private final List<long[]> gaps = new ArrayList<>();
    private final List<String> gapReasons = new ArrayList<>();
    private Map<String, String> firingAlerts = new HashMap<>();
    private final Map<Integer, StreamInfo> streams = new HashMap<>();
    private final Map<String, List<Account>> accounts = new HashMap<>();
    private final Map<String, List<TaskStatus>> statuses = new HashMap<>();
    private final Set<String> services = new java.util.HashSet<>();
    private final List<String> alertEvents = new ArrayList<>();
    private final String version = "1.23.0";
    // Set to roll: until then a newer release changes nothing for running shards.
    private String rollTo;
    private int maxStreamsPerShard = 500;
    // Stream ids asked of the registry, per lookup.
    private final List<Integer> streamLookups = new ArrayList<>();
    // A shard whose service calls throw, as when its region's base task definition isn't deployed.
    private String failingShard;

    private final Store store = new Store() {
        @Override
        public List<Rule> activeRules() {
            return List.copyOf(rules);
        }

        @Override
        public void putRuleStatus(final String ruleId, final RuleStatus status) {}

        @Override
        public void removeEventIds(final String ruleId, final Set<Integer> eventIds) {}

        @Override
        public Map<Integer, Target> targets() {
            return new HashMap<>(targets);
        }

        @Override
        public void putTarget(final Target target) {
            targets.put(target.streamId(), target);
        }

        @Override
        public void deleteTarget(final int streamId) {
            targets.remove(streamId);
        }

        @Override
        public List<ShardRecord> shards() {
            return shards.values().stream()
                    .filter(s -> s.state() != ShardRecord.State.DELETED)
                    .toList();
        }

        @Override
        public boolean putShard(final ShardRecord shard, final long expectedGeneration) {
            ShardRecord stored = shards.get(shard.shardId());
            if ((stored == null ? -1 : stored.generation()) != expectedGeneration) {
                return false;
            }
            shards.put(shard.shardId(), shard);
            return true;
        }

        @Override
        public List<Interval> openIntervals(final String shardId) {
            return intervals.stream()
                    .filter(i -> i.open() && i.shardId().equals(shardId))
                    .toList();
        }

        @Override
        public void putInterval(final Interval interval) {
            intervals.removeIf(i -> i.streamId() == interval.streamId()
                    && i.taskId().equals(interval.taskId())
                    && i.fromNanos() == interval.fromNanos());
            intervals.add(interval);
        }

        @Override
        public void putGap(final int streamId, final long fromNanos, final long toNanos, final String reason) {
            gaps.add(new long[] {streamId, fromNanos, toNanos});
            gapReasons.add(reason);
        }

        @Override
        public Map<String, String> firingAlerts() {
            return firingAlerts;
        }

        @Override
        public void putFiringAlerts(final Map<String, String> typeByKey) {
            firingAlerts = new HashMap<>(typeByKey);
        }
    };

    private final AtomicInteger shardIds = new AtomicInteger();

    private final Reconciler reconciler = new Reconciler(
            store,
            new Registry() {
                @Override
                public Map<Integer, StreamInfo> streams(final Collection<Integer> ids) {
                    streamLookups.add(ids.size());
                    return ids.stream().filter(streams::containsKey).collect(Collectors.toMap(id -> id, streams::get));
                }

                @Override
                public Map<String, SourceLimits> sources() {
                    return Map.of(
                            "KALSHI", new SourceLimits("KALSHI", 250, SourceLimits.Scope.ACCOUNT, 180, true),
                            "POLYMARKET_INTL",
                                    new SourceLimits("POLYMARKET_INTL", 250, SourceLimits.Scope.IP, 0, false));
                }

                @Override
                public Map<String, List<Account>> accounts() {
                    return accounts;
                }

                @Override
                public List<EventVolume> events(
                        final RuleSelector.EventFilter filter,
                        final boolean byVolume,
                        final double minVolume24h,
                        final long expiresBeforeMillis,
                        final int limit) {
                    return List.of();
                }

                @Override
                public Set<Integer> resolvedEvents(final Collection<Integer> eventIds) {
                    return Set.of();
                }

                @Override
                public Map<Integer, Set<Integer>> streamsOfEvents(final Collection<Integer> eventIds) {
                    return Map.of();
                }
            },
            shardId -> statuses.getOrDefault(shardId, List.of()),
            new Services() {
                @Override
                public void ensure(final ShardRecord shard) {
                    failIfAsked(shard);
                    services.add(shard.shardId() + "@" + shard.version());
                }

                @Override
                public void remove(final ShardRecord shard) {
                    failIfAsked(shard);
                    services.removeIf(service -> service.startsWith(shard.shardId() + "@"));
                }

                private void failIfAsked(final ShardRecord shard) {
                    if (shard.shardId().equals(failingShard)) {
                        throw new IllegalStateException("ECS is down");
                    }
                }
            },
            new Alerts() {
                @Override
                public void firing(
                        final String type, final String key, final String title, final Map<String, String> labels) {
                    alertEvents.add("FIRING " + key);
                }

                @Override
                public void resolved(final String type, final String key) {
                    alertEvents.add("RESOLVED " + key);
                }
            },
            () -> new Settings(
                    maxStreamsPerShard,
                    5,
                    Map.of(),
                    rollTo == null ? version : rollTo,
                    rollTo,
                    2 * MINUTE,
                    10 * MINUTE,
                    10 * MINUTE,
                    5 * MINUTE,
                    5 * MINUTE),
            partition -> "use1-" + shardIds.incrementAndGet());

    ReconcilerTest() {
        for (int id = 1; id <= 3; id++) {
            streams.put(id, new StreamInfo(id, "KALSHI", "VENUE", "us-east-1", true));
        }
        accounts.put("KALSHI", List.of(new Account("KALSHI", "md-1", 0, true)));
    }

    private void wantStreams(final Integer... ids) {
        rules.clear();
        if (ids.length > 0) {
            rules.add(new Rule("r1", Set.of(ids), Priority.RESEARCH, "standard", null));
        }
    }

    /** The task's status as of {@code at}, every stream on the shard in {@code state}. */
    private void report(
            final String shardId,
            final String task,
            final long at,
            final String state,
            final Transition... transitions) {
        ShardRecord shard = shards.get(shardId);
        List<StreamState> states = new ArrayList<>();
        shard.streams().forEach(id -> states.add(new StreamState(id, state)));
        shard.draining().forEach(id -> states.add(new StreamState(id, state)));
        statuses.put(shardId, List.of(new TaskStatus(shardId, task, at, states, List.of(transitions))));
    }

    // ----- tests -----

    @Test
    void aRulesStreamsAreGivenAShardThatIsStartedAndBoundToAnAccount() {
        wantStreams(1, 2);

        Reconciler.Summary summary = reconciler.tick(T0);

        assertEquals(2, summary.placed());
        ShardRecord shard = shards.get("use1-1");
        assertEquals(List.of(1, 2), shard.streams());
        assertEquals(Map.of("KALSHI", "md-1"), shard.accounts());
        assertEquals(1, shard.generation());
        assertEquals("1.23.0", shard.version());
        assertEquals(Set.of("use1-1@1.23.0"), services);
        assertEquals("use1-1", targets.get(1).shardId());
    }

    @Test
    void theTimeBeforeAStreamFirstWentLiveIsAGapAndThenItIsCovered() {
        wantStreams(1);
        reconciler.tick(T0);
        report("use1-1", "t1", T0 + 2 * MINUTE, "LIVE", new Transition(1, "SUBSCRIBING", "LIVE", T0 + 90 * SECOND));

        reconciler.tick(T0 + 2 * MINUTE);

        assertEquals(1, gaps.size());
        assertEquals(T0, gaps.get(0)[1]);
        assertEquals(T0 + 90 * SECOND, gaps.get(0)[2]);
        assertEquals(Target.NOT_PLACED, gapReasons.get(0));
        assertEquals(0, targets.get(1).uncoveredSinceNanos(), "covered now");
        assertTrue(intervals.get(0).open());
    }

    @Test
    void aStreamThatWentStaleIsAGapFromWhenItDidUntilItIsLiveAgain() {
        wantStreams(1);
        reconciler.tick(T0);
        report("use1-1", "t1", T0 + MINUTE, "LIVE");
        reconciler.tick(T0 + MINUTE);
        report("use1-1", "t1", T0 + 2 * MINUTE, "STALE", new Transition(1, "LIVE", "STALE", T0 + 100 * SECOND));
        reconciler.tick(T0 + 2 * MINUTE);
        report("use1-1", "t1", T0 + 3 * MINUTE, "LIVE", new Transition(1, "STALE", "LIVE", T0 + 130 * SECOND));

        reconciler.tick(T0 + 3 * MINUTE);

        long[] gap = gaps.get(gaps.size() - 1);
        assertEquals(T0 + 100 * SECOND, gap[1]);
        assertEquals(T0 + 130 * SECOND, gap[2]);
        assertEquals("STALE", gapReasons.get(gapReasons.size() - 1));
    }

    @Test
    void anUnwantedStreamRetiresAfterGraceAndAnEmptyShardScalesToZero() {
        wantStreams(1);
        reconciler.tick(T0);
        wantStreams();
        reconciler.tick(T0 + MINUTE);
        assertEquals(Target.State.RETIRING, targets.get(1).state());

        reconciler.tick(T0 + 6 * MINUTE);
        assertNull(targets.get(1), "retired and deleted");
        assertTrue(shards.get("use1-1").isEmpty());

        reconciler.tick(T0 + 17 * MINUTE);
        assertEquals(ShardRecord.State.DELETED, shards.get("use1-1").state(), "empty for 10 minutes");
        assertTrue(services.isEmpty());
    }

    private String moveToAnotherAccount() {
        accounts.put("KALSHI", List.of(new Account("KALSHI", "md-1", 0, true), new Account("KALSHI", "md-2", 0, true)));
        wantStreams(1);
        reconciler.tick(T0);
        String first = targets.get(1).shardId();
        String firstLabel = shards.get(first).accounts().get("KALSHI");
        report(first, "t1", T0 + MINUTE, "LIVE");
        accounts.put(
                "KALSHI",
                List.of(
                        new Account("KALSHI", firstLabel, 0, false),
                        new Account("KALSHI", firstLabel.equals("md-1") ? "md-2" : "md-1", 0, true)));
        reconciler.tick(T0 + MINUTE);
        return first;
    }

    @Test
    void aHandOffCutsAtAMinuteSoEachMinuteIsWrittenOnce() {
        String first = moveToAnotherAccount();
        String second = targets.get(1).shardId();
        assertFalse(first.equals(second));
        assertEquals(List.of(1), shards.get(first).draining(), "the old shard still collects and writes it");
        assertEquals(Map.of(), shards.get(first).cuts());
        assertEquals(List.of(1), shards.get(second).streams());
        assertEquals(
                Map.of(1, ShardRecord.WARMING), shards.get(second).cuts(), "the new one collects, writing nothing");

        report(first, "t1", T0 + 2 * MINUTE, "LIVE");
        report(second, "t2", T0 + 2 * MINUTE, "LIVE");
        reconciler.tick(T0 + 2 * MINUTE);
        assertEquals(Map.of(1, ShardRecord.WARMING), shards.get(second).cuts(), "covered, but not yet for two minutes");

        report(first, "t1", T0 + 4 * MINUTE, "LIVE");
        report(second, "t2", T0 + 4 * MINUTE, "LIVE");
        reconciler.tick(T0 + 4 * MINUTE + 10 * SECOND);
        int cut = (int) ((T0 + 6 * MINUTE) / MINUTE);
        assertEquals(Map.of(1, cut), shards.get(second).cuts(), "the first whole minute at least a minute ahead");
        assertEquals(Map.of(1, cut), shards.get(first).cuts(), "the old shard writes up to the same minute");
        assertEquals(List.of(1), shards.get(first).draining());

        report(first, "t1", T0 + 7 * MINUTE, "LIVE");
        report(second, "t2", T0 + 7 * MINUTE, "LIVE");
        reconciler.tick(T0 + 7 * MINUTE);
        assertEquals(List.of(1), shards.get(first).draining(), "kept until its last minute has closed");

        reconciler.tick(T0 + 8 * MINUTE);
        assertTrue(shards.get(first).isEmpty(), "handed off");
        assertEquals(Map.of(), shards.get(first).cuts());
        assertEquals(Map.of(), shards.get(second).cuts(), "the new shard writes every minute from here");
        assertNull(targets.get(1).drainingFromShardId());
    }

    @Test
    void ifTheOldShardStopsCoveringTheCutMovesToThisMinuteAndTheOldShardKeepsItUntilSettled() {
        String first = moveToAnotherAccount();
        String second = targets.get(1).shardId();

        report(first, "t1", T0 + 2 * MINUTE, "STALE");
        report(second, "t2", T0 + 2 * MINUTE, "LIVE");
        reconciler.tick(T0 + 2 * MINUTE + 30 * SECOND);

        int thisMinute = (int) ((T0 + 2 * MINUTE) / MINUTE);
        assertEquals(Map.of(1, thisMinute), shards.get(second).cuts(), "the new shard writes from this minute");
        assertEquals(Map.of(1, thisMinute), shards.get(first).cuts(), "the old one writes nothing more");
        assertEquals(List.of(1), shards.get(first).draining(), "and lets go only once its last minute has closed");

        report(second, "t2", T0 + 4 * MINUTE, "LIVE");
        reconciler.tick(T0 + 4 * MINUTE);
        assertTrue(shards.get(first).isEmpty());
        assertEquals(Map.of(), shards.get(second).cuts());
    }

    @Test
    void aHandOffNeverCompletesBeforeTheNewShardCovers() {
        String first = moveToAnotherAccount();

        report(first, "t1", T0 + 2 * MINUTE, "STALE");
        reconciler.tick(T0 + 2 * MINUTE);

        assertEquals(List.of(1), shards.get(first).draining(), "make before break: nothing has it yet");
        assertEquals(0, targets.get(1).cutMinute());
    }

    @Test
    void ifTheNewShardLosesTheStreamBeforeTheCutTheHandOffStartsOver() {
        String first = moveToAnotherAccount();
        String second = targets.get(1).shardId();
        report(first, "t1", T0 + 2 * MINUTE, "LIVE");
        report(second, "t2", T0 + 2 * MINUTE, "LIVE");
        reconciler.tick(T0 + 2 * MINUTE);
        report(first, "t1", T0 + 4 * MINUTE, "LIVE");
        report(second, "t2", T0 + 4 * MINUTE, "LIVE");
        reconciler.tick(T0 + 4 * MINUTE + 10 * SECOND);
        assertEquals(1, shards.get(first).cuts().size(), "cut");

        report(first, "t1", T0 + 5 * MINUTE, "LIVE");
        report(second, "t2", T0 + 5 * MINUTE, "STALE");
        reconciler.tick(T0 + 5 * MINUTE);

        assertEquals(Map.of(), shards.get(first).cuts(), "the old shard writes every minute again");
        assertEquals(Map.of(1, ShardRecord.WARMING), shards.get(second).cuts());
        assertEquals(List.of(1), shards.get(first).draining());
        assertEquals(0, targets.get(1).cutMinute());
    }

    @Test
    void aShardItemThatMissedAWriteIsRebuiltFromTheTargets() {
        wantStreams(1, 2);
        reconciler.tick(T0);
        ShardRecord stored = shards.get("use1-1");
        shards.put(
                "use1-1",
                new ShardRecord(
                        stored.shardId(),
                        stored.partition(),
                        stored.generation(),
                        List.of(1),
                        List.of(),
                        stored.accounts(),
                        Map.of(),
                        stored.version(),
                        stored.state(),
                        stored.createdAtNanos(),
                        stored.emptySinceNanos()));

        reconciler.tick(T0 + MINUTE);

        assertEquals(List.of(1, 2), shards.get("use1-1").streams());
    }

    @Test
    void aReplacedTaskDoesNotLeaveItsStreamsUncovered() {
        wantStreams(1);
        reconciler.tick(T0);
        report("use1-1", "t1", T0 + MINUTE, "LIVE");
        reconciler.tick(T0 + MINUTE);
        assertEquals(0, targets.get(1).uncoveredSinceNanos());

        // t1 dies after its status at T0+1m; t2 replaces it and is live from T0+2m.
        statuses.put(
                "use1-1",
                List.of(new TaskStatus(
                        "use1-1", "t2", T0 + 2 * MINUTE, List.of(new StreamState(1, "LIVE")), List.of())));
        reconciler.tick(T0 + 2 * MINUTE);
        reconciler.tick(T0 + 7 * MINUTE);
        statuses.put(
                "use1-1",
                List.of(new TaskStatus(
                        "use1-1", "t2", T0 + 8 * MINUTE, List.of(new StreamState(1, "LIVE")), List.of())));
        reconciler.tick(T0 + 8 * MINUTE);

        assertEquals(0, targets.get(1).uncoveredSinceNanos(), "t1's late close doesn't uncover what t2 covers");
    }

    @Test
    void aRaisedPriorityMovesTheStreamToAShardOfItsNewPartition() {
        wantStreams(1);
        reconciler.tick(T0);
        report("use1-1", "t1", T0 + MINUTE, "LIVE");
        rules.clear();
        rules.add(new Rule("r1", Set.of(1), Priority.PRODUCTION, "standard", null));

        reconciler.tick(T0 + MINUTE);

        String moved = targets.get(1).shardId();
        assertFalse(moved.equals("use1-1"));
        assertEquals(Priority.PRODUCTION, shards.get(moved).partition().priority());
        assertEquals(List.of(1), shards.get("use1-1").draining());
    }

    @Test
    void aFailingShardServiceDoesNotStopTheOthers() {
        wantStreams(1);
        reconciler.tick(T0);
        failingShard = "use1-1";
        rules.clear();
        rules.add(new Rule("r1", Set.of(1), Priority.RESEARCH, "standard", null));
        rules.add(new Rule("r2", Set.of(2), Priority.PRODUCTION, "standard", null));

        Reconciler.Summary summary = reconciler.tick(T0 + MINUTE);

        assertEquals(1, summary.serviceErrors());
        assertTrue(services.contains("use1-2@" + version), "the other shard still started");
    }

    @Test
    void aShardWhoseServiceCannotBeRemovedIsRetriedNotForgotten() {
        wantStreams(1);
        reconciler.tick(T0);
        wantStreams();
        reconciler.tick(T0 + MINUTE);
        reconciler.tick(T0 + 6 * MINUTE);
        failingShard = "use1-1";

        reconciler.tick(T0 + 17 * MINUTE);
        assertNotEquals(ShardRecord.State.DELETED, shards.get("use1-1").state(), "not marked deleted while it runs");

        failingShard = null;
        reconciler.tick(T0 + 18 * MINUTE);
        assertEquals(ShardRecord.State.DELETED, shards.get("use1-1").state());
        assertTrue(services.isEmpty());
    }

    @Test
    void aNewVersionRollsOneShardAtATime() {
        wantStreams(1, 2);
        reconciler.tick(T0);
        rollTo = "1.24.0";

        reconciler.tick(T0 + MINUTE);

        assertEquals(ShardRecord.State.DRAINING, shards.get("use1-1").state());
        assertEquals(List.of(1, 2), shards.get("use1-1").draining());
        assertEquals("1.24.0", shards.get("use1-2").version());
        assertTrue(services.contains("use1-2@1.24.0"));

        report("use1-1", "t1", T0 + 2 * MINUTE, "LIVE");
        report("use1-2", "t2", T0 + 2 * MINUTE, "LIVE");
        reconciler.tick(T0 + 2 * MINUTE);
        report("use1-1", "t1", T0 + 4 * MINUTE, "LIVE");
        report("use1-2", "t2", T0 + 4 * MINUTE, "LIVE");
        reconciler.tick(T0 + 4 * MINUTE);
        reconciler.tick(T0 + 8 * MINUTE);
        assertEquals(ShardRecord.State.DELETED, shards.get("use1-1").state(), "drained, then deleted");
        assertEquals(Set.of("use1-2@1.24.0"), services);
    }

    @Test
    void aRollMovesStreamsOnlyOntoShardsAlreadyOnTheNewVersion() {
        maxStreamsPerShard = 2;
        wantStreams(1, 2, 3);
        reconciler.tick(T0);
        assertEquals(List.of(1, 2), shards.get("use1-1").streams());
        assertEquals(List.of(3), shards.get("use1-2").streams(), "room for one more");
        rollTo = "1.24.0";

        reconciler.tick(T0 + MINUTE);

        assertEquals(ShardRecord.State.DRAINING, shards.get("use1-1").state());
        assertEquals(List.of(3), shards.get("use1-2").streams(), "an old-version shard takes nothing during the roll");
        assertEquals(List.of(1, 2), shards.get("use1-3").streams());
        assertEquals("1.24.0", shards.get("use1-3").version());
    }

    @Test
    void streamInfoIsReadOnceEveryFewMinutesNotEveryTick() {
        wantStreams(1, 2);
        reconciler.tick(T0);
        reconciler.tick(T0 + MINUTE);
        reconciler.tick(T0 + 2 * MINUTE);
        assertEquals(List.of(2), streamLookups, "the next ticks reuse what the first read");

        reconciler.tick(T0 + 5 * MINUTE);
        assertEquals(List.of(2, 2), streamLookups);
    }

    @Test
    void aNewerReleaseAloneRollsNothing() {
        wantStreams(1);
        reconciler.tick(T0);

        reconciler.tick(T0 + MINUTE);

        assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-1").state());
        assertEquals(1, shards.size());
    }

    @Test
    void aShardWithNoStatusRaisesAnAlertThatResolvesWhenItReports() {
        wantStreams(1);
        reconciler.tick(T0);

        reconciler.tick(T0 + 6 * MINUTE);
        assertTrue(alertEvents.contains("FIRING COLLECTOR_SHARD_DOWN:use1-1"));

        report("use1-1", "t1", T0 + 7 * MINUTE, "LIVE");
        reconciler.tick(T0 + 7 * MINUTE);
        assertTrue(alertEvents.contains("RESOLVED COLLECTOR_SHARD_DOWN:use1-1"));
    }

    @Test
    void streamsNotLiveAlertOncePerSourceAndRegion() {
        wantStreams(1, 2, 3);
        reconciler.tick(T0);
        report("use1-1", "t1", T0 + 11 * MINUTE, "SUBSCRIBING");

        reconciler.tick(T0 + 11 * MINUTE);

        assertEquals(
                1,
                alertEvents.stream()
                        .filter(e -> e.startsWith("FIRING STREAM_NOT_LIVE"))
                        .count());
        assertTrue(firingAlerts.containsKey("STREAM_NOT_LIVE:KALSHI:us-east-1"));
    }

    @Test
    void streamsNoAccountHasRoomForAlertAsBudget() {
        accounts.put("KALSHI", List.of(new Account("KALSHI", "md-1", 0, false)));
        wantStreams(1);

        Reconciler.Summary summary = reconciler.tick(T0);

        assertEquals(1, summary.unplaced());
        assertTrue(alertEvents.contains("FIRING COLLECTION_BUDGET:KALSHI"));
    }

    @Test
    void aWriteThatLostTheRaceIsLeftForTheNextTick() {
        wantStreams(1);
        reconciler.tick(T0);
        ShardRecord stored = shards.get("use1-1");
        shards.put(
                "use1-1",
                new ShardRecord(
                        stored.shardId(),
                        stored.partition(),
                        9,
                        stored.streams(),
                        stored.draining(),
                        stored.accounts(),
                        stored.cuts(),
                        stored.version(),
                        stored.state(),
                        stored.createdAtNanos(),
                        stored.emptySinceNanos()));
        wantStreams(1, 2);

        reconciler.tick(T0 + MINUTE);

        assertEquals(10, shards.get("use1-1").generation(), "read the moved generation and applied on top of it");
        assertEquals(List.of(1, 2), shards.get("use1-1").streams());
    }
}
