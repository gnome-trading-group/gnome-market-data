package group.gnometrading.collection.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.EventVolume;
import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.Lease;
import group.gnometrading.collection.model.Partition;
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
    private static final Partition RESEARCH_US = new Partition("us-east-1", "VENUE", Priority.RESEARCH, "standard");

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
    private boolean consolidate;
    private boolean dryRun;
    private int maxStreamsPerShard = 500;
    // Stream ids asked of the registry, per lookup.
    private final List<Integer> streamLookups = new ArrayList<>();
    // A shard whose service calls throw, as when its region's base task definition isn't deployed.
    private String failingShard;
    private final Map<String, Lease> leases = new LinkedHashMap<>();
    // Each released lease's reason, by session.
    private final Map<String, String> releasedLeases = new HashMap<>();
    // Session statuses as the registry reports them; a session missing here is one it doesn't know.
    private final Map<String, String> sessionStatuses = new HashMap<>();
    private boolean registryDown;
    // The mode each read of the registry's sessions asked for ("any" for none).
    private final List<String> sessionReads = new ArrayList<>();
    private final Map<Partition, Integer> minShards = new HashMap<>();
    private final Map<Partition, Integer> maxShards = new HashMap<>();

    private final Store store = new Store() {
        @Override
        public List<Rule> activeRules() {
            return List.copyOf(rules);
        }

        @Override
        public void putRuleStatus(final String ruleId, final RuleStatus status) {}

        @Override
        public List<Lease> leases() {
            return List.copyOf(leases.values());
        }

        @Override
        public void releaseLease(final String sessionId, final String reason, final long nowSeconds) {
            leases.put(sessionId, leases.get(sessionId).released(nowSeconds));
            releasedLeases.put(sessionId, reason);
        }

        @Override
        public void recordLeaseSession(
                final String sessionId, final String sessionStatus, final long seenAtSeconds, final long expiresAt) {
            leases.put(sessionId, leases.get(sessionId).withSession(sessionStatus, seenAtSeconds, expiresAt));
        }

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
                public Map<String, String> openSessionStatuses(final String mode) {
                    sessionReads.add(mode == null ? "any" : mode);
                    if (registryDown) {
                        throw new IllegalStateException("registry is down");
                    }
                    Map<String, String> open = new HashMap<>();
                    sessionStatuses.forEach((id, status) -> {
                        if (!status.equals("STOPPED") && !status.equals("FAILED")) {
                            open.put(id, status);
                        }
                    });
                    return open;
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
                    5 * MINUTE,
                    consolidate,
                    dryRun,
                    15 * MINUTE,
                    0.8,
                    Map.copyOf(minShards),
                    Map.copyOf(maxShards)),
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

    private static long seconds(final long nanos) {
        return TimeUnit.NANOSECONDS.toSeconds(nanos);
    }

    /**
     * A lease made at {@code at}, expiring two hours later, as the lease API writes it; its session STARTING unless the
     * test said otherwise.
     */
    private void lease(final String sessionId, final String mode, final long at, final Integer... ids) {
        sessionStatuses.putIfAbsent(sessionId, "STARTING");
        leases.put(
                sessionId,
                Lease.active(
                        sessionId,
                        Set.of(ids),
                        mode.equals("live") ? Priority.PRODUCTION : Priority.RESEARCH,
                        "standard",
                        mode,
                        seconds(at),
                        seconds(at) + TimeUnit.HOURS.toSeconds(2)));
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
                        List.of(),
                        stored.version(),
                        stored.state(),
                        stored.createdAtNanos(),
                        stored.emptySinceNanos(),
                        stored.mergeableSinceNanos()));

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

    /**
     * Two shards in one partition at 4 streams each at most (3 after a merge, at 0.8): use1-1 holds 1–4, use1-2 holds
     * 5–6. Streams 1–3 then retire, leaving use1-1 with only stream 4, which fits on use1-2. Returns when the
     * retirement is done.
     */
    private long twoUnderFilledShards() {
        consolidate = true;
        maxStreamsPerShard = 4;
        for (int id = 4; id <= 7; id++) {
            streams.put(id, new StreamInfo(id, "KALSHI", "VENUE", "us-east-1", true));
        }
        wantStreams(1, 2, 3, 4, 5, 6);
        reconciler.tick(T0);
        assertEquals(List.of(1, 2, 3, 4), shards.get("use1-1").streams());
        assertEquals(List.of(5, 6), shards.get("use1-2").streams());
        wantStreams(4, 5, 6);
        for (long at = T0 + MINUTE; at <= T0 + 7 * MINUTE; at += MINUTE) {
            reconciler.tick(at);
        }
        assertEquals(List.of(4), shards.get("use1-1").streams(), "1–3 retired after their grace");
        return T0 + 7 * MINUTE;
    }

    @Test
    void anUnderFilledShardMergesIntoItsPartitionsOthersAfterStayingMergeable() {
        long at = twoUnderFilledShards();
        long since = shards.get("use1-1").mergeableSinceNanos();
        assertTrue(since > 0, "use1-1's one stream fits on use1-2");
        assertTrue(
                shards.get("use1-2").mergeableSinceNanos() > 0, "and use1-2's two on use1-1; the emptier goes first");

        for (at += MINUTE; at < since + 15 * MINUTE; at += MINUTE) {
            reconciler.tick(at);
            assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-1").state(), "not yet fifteen minutes");
        }
        Reconciler.Summary summary = reconciler.tick(at);

        assertEquals(1, summary.merged());
        assertEquals(ShardRecord.State.DRAINING, shards.get("use1-1").state());
        assertEquals(List.of(4), shards.get("use1-1").draining(), "handed off, not dropped");
        assertEquals(
                List.of(4, 5, 6),
                shards.get("use1-2").streams().stream().sorted().toList());
        assertEquals(2, shards.size(), "no shard created for the merge");

        for (int i = 1; i <= 6; i++) {
            report("use1-1", "t1", at + i * MINUTE, "LIVE");
            report("use1-2", "t2", at + i * MINUTE, "LIVE");
            reconciler.tick(at + i * MINUTE);
        }
        assertEquals(ShardRecord.State.DELETED, shards.get("use1-1").state(), "drained, then deleted");
        assertEquals(Set.of("use1-2@1.23.0"), services);
    }

    @Test
    void theEmptiestMergeableShardIsWaitedForEvenIfAFullerOneQualifiedFirst() {
        long at = twoUnderFilledShards();
        long emptierSince = shards.get("use1-1").mergeableSinceNanos();
        // use1-2's two streams fit on use1-1 too; pretend that has held for longer.
        shards.put("use1-2", shards.get("use1-2").withMergeableSince(emptierSince - 10 * MINUTE));

        reconciler.tick(emptierSince + 10 * MINUTE);
        assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-2").state(), "not the fuller one, though it qualified");
        assertEquals(
                ShardRecord.State.ACTIVE, shards.get("use1-1").state(), "and the emptier hasn't waited long enough");

        reconciler.tick(emptierSince + 15 * MINUTE);
        assertEquals(
                ShardRecord.State.DRAINING, shards.get("use1-1").state(), "its one stream moves, not the other's two");
        assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-2").state());
    }

    @Test
    void aDryRunOnlyNotesTheMerge() {
        long at = twoUnderFilledShards();
        dryRun = true;

        Reconciler.Summary summary = reconciler.tick(at + 20 * MINUTE);

        assertEquals(0, summary.merged());
        assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-1").state());
        assertTrue(shards.get("use1-1").mergeableSinceNanos() > 0, "still shown as mergeable");
    }

    @Test
    void aShardThatStopsFittingIsNoLongerMergeable() {
        long at = twoUnderFilledShards();
        wantStreams(4, 5, 6, 7);

        // Consolidation runs before placement, so it sees the new stream placed on the next tick (and waits for an
        // unplaced one meanwhile).
        reconciler.tick(at + MINUTE);
        reconciler.tick(at + 2 * MINUTE);

        assertEquals(List.of(5, 6, 7), shards.get("use1-2").streams(), "the new stream goes on the fuller shard");
        assertEquals(0, shards.get("use1-1").mergeableSinceNanos(), "its stream would leave use1-2 over 0.8");
        assertEquals(0, shards.get("use1-2").mergeableSinceNanos());
        reconciler.tick(at + 30 * MINUTE);
        assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-1").state());
    }

    @Test
    void nothingMergesDuringARoll() {
        long at = twoUnderFilledShards();
        rollTo = "1.24.0";

        reconciler.tick(at + 20 * MINUTE);

        assertEquals(
                0,
                shards.values().stream()
                        .mapToInt(shard -> shard.mergeableSinceNanos() > 0 ? 0 : 0)
                        .sum());
        assertTrue(
                shards.values().stream()
                        .noneMatch(shard -> shard.state() == ShardRecord.State.DRAINING
                                && shard.version().equals("1.23.0")
                                && shard.shardId().equals("use1-2")),
                "the roll drains its oldest shard; no merge drains another alongside it");
    }

    @Test
    void consolidationOffForgetsMergeableShards() {
        long at = twoUnderFilledShards();
        consolidate = false;

        reconciler.tick(at + 20 * MINUTE);

        assertEquals(0, shards.get("use1-1").mergeableSinceNanos());
        assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-1").state());
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
                        stored.publish(),
                        stored.version(),
                        stored.state(),
                        stored.createdAtNanos(),
                        stored.emptySinceNanos(),
                        stored.mergeableSinceNanos()));
        wantStreams(1, 2);

        reconciler.tick(T0 + MINUTE);

        assertEquals(10, shards.get("use1-1").generation(), "read the moved generation and applied on top of it");
        assertEquals(List.of(1, 2), shards.get("use1-1").streams());
    }

    // ----- leases -----

    @Test
    void aLiveLeasePlacesItsStreamOnAProductionShardThatPublishesIt() {
        lease("s1", "live", T0, 1);

        Reconciler.Summary summary = reconciler.tick(T0);

        Target target = targets.get(1);
        assertEquals(Priority.PRODUCTION, target.partition().priority());
        assertEquals(Set.of("lease:s1"), target.wantedBy());
        assertEquals(5, target.retireGraceMinutes(), "a restarting session keeps its shard for a while");
        ShardRecord shard = shards.get(target.shardId());
        assertEquals(Priority.PRODUCTION, shard.partition().priority());
        assertEquals(List.of(1), shard.publish());
        assertEquals(1, summary.leases());
    }

    @Test
    void aPaperLeaseSharesTheRulesResearchShardWhichPublishesOnlyTheLeasedStream() {
        wantStreams(1, 2);
        lease("s1", "paper", T0, 2);

        reconciler.tick(T0);

        ShardRecord shard = shards.get("use1-1");
        assertEquals(List.of(1, 2), shard.streams());
        assertEquals(List.of(2), shard.publish());
        assertEquals(Set.of("lease:s1", "r1"), targets.get(2).wantedBy());
        assertNull(targets.get(2).retireGraceMinutes(), "the rule's grace (the default), not the lease's");
        assertEquals(1, shards.size(), "no shard of its own");
    }

    @Test
    void bothShardsPublishALeasedStreamWhileItHandsOff() {
        lease("s1", "paper", T0, 1);
        String first = moveToAnotherAccount();
        String second = targets.get(1).shardId();

        assertEquals(List.of(1), shards.get(first).draining());
        assertEquals(List.of(1), shards.get(first).publish(), "the session reads the old shard until the switch");
        assertEquals(List.of(1), shards.get(second).publish());
    }

    @Test
    void aLeaseWhoseSessionStoppedIsReleasedButStaysPublishedForTheLeaseGrace() {
        lease("s1", "paper", T0, 1);
        sessionStatuses.put("s1", "RUNNING");
        reconciler.tick(T0);
        sessionStatuses.put("s1", "STOPPED");

        Reconciler.Summary summary = reconciler.tick(T0 + MINUTE);

        assertEquals(Map.of("s1", "SESSION_ENDED"), releasedLeases);
        assertEquals(1, summary.releasedLeases());
        assertEquals(Target.State.RETIRING, targets.get(1).state());
        assertEquals(List.of(1), shards.get("use1-1").publish(), "the stopping session still reads it");

        reconciler.tick(T0 + 5 * MINUTE);
        assertEquals(List.of(1), shards.get("use1-1").publish());

        reconciler.tick(T0 + 6 * MINUTE + SECOND);
        assertNull(targets.get(1));
        assertEquals(List.of(), shards.get("use1-1").publish());
    }

    @Test
    void aStreamStaysPublishedWhileAnotherLeaseWantsItAndStopsAfterTheLastOnesGrace() {
        wantStreams(1);
        lease("s1", "paper", T0, 1);
        lease("s2", "paper", T0, 1);
        sessionStatuses.put("s1", "RUNNING");
        sessionStatuses.put("s2", "RUNNING");
        reconciler.tick(T0);
        sessionStatuses.put("s1", "STOPPED");

        reconciler.tick(T0 + MINUTE);
        reconciler.tick(T0 + 10 * MINUTE);
        assertEquals(List.of(1), shards.get("use1-1").publish(), "s2 still wants it, long after s1's grace");

        sessionStatuses.put("s2", "FAILED");
        reconciler.tick(T0 + 11 * MINUTE);
        assertEquals(List.of(1), shards.get("use1-1").publish(), "in s2's grace");

        reconciler.tick(T0 + 16 * MINUTE + SECOND);
        assertEquals(List.of(), shards.get("use1-1").publish());
        assertEquals(List.of(1), shards.get("use1-1").streams(), "the rule still collects it");
    }

    @Test
    void aFailedSessionsLeaseIsReleasedToo() {
        lease("s1", "paper", T0, 1);
        sessionStatuses.put("s1", "FAILED");

        reconciler.tick(T0);

        assertEquals(Map.of("s1", "SESSION_ENDED"), releasedLeases);
        assertNull(targets.get(1), "never wanted");
    }

    @Test
    void theRegistryIsReadOnceATickInTheLeasesMode() {
        lease("s1", "paper", T0, 1);
        lease("s2", "paper", T0, 2);
        lease("s3", "paper", T0, 3);
        sessionStatuses.put("s1", "RUNNING");
        sessionStatuses.put("s2", "RUNNING");
        sessionStatuses.put("s3", "RUNNING");

        reconciler.tick(T0);
        assertEquals(List.of("paper"), sessionReads);

        lease("s4", "live", T0, 1);
        sessionStatuses.put("s4", "RUNNING");
        reconciler.tick(T0 + MINUTE);
        assertEquals(List.of("paper", "any"), sessionReads, "mixed modes ask for every mode");
    }

    @Test
    void aLeaseWhoseSessionTheRegistryDoesNotListHasEnded() {
        lease("s1", "paper", T0, 1);
        sessionStatuses.remove("s1");

        reconciler.tick(T0);

        assertEquals(Map.of("s1", "SESSION_ENDED"), releasedLeases);
    }

    @Test
    void aLeasePastItsExpiryIsReleasedUnlessItsSessionRuns() {
        lease("s1", "paper", T0, 1);
        lease("s2", "paper", T0, 2);
        sessionStatuses.put("s1", "STARTING");
        sessionStatuses.put("s2", "RUNNING");

        reconciler.tick(T0 + 2 * TimeUnit.HOURS.toNanos(1) + MINUTE);

        assertEquals(Map.of("s1", "EXPIRED"), releasedLeases, "s1 never started");
        assertEquals(
                seconds(T0 + 2 * TimeUnit.HOURS.toNanos(1) + MINUTE) + TimeUnit.HOURS.toSeconds(2),
                leases.get("s2").expiresAtSeconds(),
                "extended while it runs");
    }

    @Test
    void aRunningSessionsLeaseIsExtendedOnlyOnceItHasLessThanAnHourLeft() {
        lease("s1", "paper", T0, 1);
        sessionStatuses.put("s1", "RUNNING");

        reconciler.tick(T0 + 30 * MINUTE);
        assertEquals(seconds(T0) + TimeUnit.HOURS.toSeconds(2), leases.get("s1").expiresAtSeconds());

        reconciler.tick(T0 + 61 * MINUTE);
        assertEquals(
                seconds(T0 + 61 * MINUTE) + TimeUnit.HOURS.toSeconds(2),
                leases.get("s1").expiresAtSeconds());
    }

    @Test
    void eachLeaseRecordsItsSessionsStatusWhenItChangesAndEveryFewMinutes() {
        lease("s1", "paper", T0, 1);
        sessionStatuses.put("s1", "STARTING");

        reconciler.tick(T0);
        assertEquals("STARTING", leases.get("s1").sessionStatus());
        assertEquals(seconds(T0), leases.get("s1").sessionSeenAtSeconds());

        reconciler.tick(T0 + MINUTE);
        assertEquals(seconds(T0), leases.get("s1").sessionSeenAtSeconds(), "unchanged, so not rewritten");

        sessionStatuses.put("s1", "RUNNING");
        reconciler.tick(T0 + 2 * MINUTE);
        assertEquals("RUNNING", leases.get("s1").sessionStatus());

        reconciler.tick(T0 + 7 * MINUTE);
        assertEquals(seconds(T0 + 7 * MINUTE), leases.get("s1").sessionSeenAtSeconds());
    }

    @Test
    void withTheRegistryDownALeaseNeverSeenRunningIsKeptUntilItExpires() {
        lease("s1", "paper", T0, 1);
        registryDown = true;

        reconciler.tick(T0 + MINUTE);
        assertTrue(releasedLeases.isEmpty());
        assertEquals(Target.State.DESIRED, targets.get(1).state());

        reconciler.tick(T0 + 121 * MINUTE);
        assertEquals(Map.of("s1", "EXPIRED"), releasedLeases);
    }

    @Test
    void withTheRegistryDownARunningSessionsLeaseOutlivesItsExpiryAndAlerts() {
        lease("s1", "paper", T0, 1);
        lease("s2", "paper", T0, 2);
        sessionStatuses.put("s1", "RUNNING");
        sessionStatuses.put("s2", "SUBMITTED");
        reconciler.tick(T0);
        registryDown = true;

        reconciler.tick(T0 + 14 * MINUTE);
        assertFalse(firingAlerts.containsKey("LEASE_HOLDERS_UNREADABLE"));
        reconciler.tick(T0 + 15 * MINUTE);
        assertTrue(alertEvents.contains("FIRING LEASE_HOLDERS_UNREADABLE"));

        reconciler.tick(T0 + 3 * TimeUnit.HOURS.toNanos(1));
        assertEquals(Map.of("s2", "EXPIRED"), releasedLeases, "s1 was last seen running");
        assertEquals(Target.State.DESIRED, targets.get(1).state());

        registryDown = false;
        reconciler.tick(T0 + 3 * TimeUnit.HOURS.toNanos(1) + MINUTE);
        assertTrue(alertEvents.contains("RESOLVED LEASE_HOLDERS_UNREADABLE"));
        assertEquals(
                seconds(T0 + 3 * TimeUnit.HOURS.toNanos(1) + MINUTE) + TimeUnit.HOURS.toSeconds(2),
                leases.get("s1").expiresAtSeconds(),
                "extended once the registry answers");
    }

    @Test
    void minShardsKeepsAnEmptyShardRunning() {
        minShards.put(new Partition("us-east-1", "VENUE", Priority.RESEARCH, "standard"), 1);
        wantStreams(1);
        reconciler.tick(T0);
        wantStreams();
        reconciler.tick(T0 + MINUTE);
        reconciler.tick(T0 + 6 * MINUTE);
        assertTrue(shards.get("use1-1").isEmpty());

        reconciler.tick(T0 + 30 * MINUTE);

        assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-1").state(), "kept past the empty-shard time");
        assertEquals(1, shards.size(), "and none started besides");
    }

    @Test
    void minShardsStartsAShardInAPartitionWithNone() {
        Partition production = new Partition("us-east-1", "VENUE", Priority.PRODUCTION, "standard");
        minShards.put(production, 1);

        reconciler.tick(T0);

        assertEquals(1, shards.size());
        ShardRecord warm = shards.get("use1-1");
        assertEquals(production, warm.partition());
        assertEquals(Set.of("use1-1@" + version), services);

        lease("s1", "live", T0 + MINUTE, 1);
        reconciler.tick(T0 + MINUTE);
        assertEquals("use1-1", targets.get(1).shardId(), "the lease lands on the warm shard");
    }

    @Test
    void minShardsAboveOneLetsOnlyTheSurplusScaleDown() {
        Partition research = new Partition("us-east-1", "VENUE", Priority.RESEARCH, "standard");
        maxStreamsPerShard = 1;
        wantStreams(1, 2);
        reconciler.tick(T0);
        assertEquals(2, shards.size());
        minShards.put(research, 1);
        wantStreams();

        reconciler.tick(T0 + MINUTE);
        reconciler.tick(T0 + 6 * MINUTE);
        reconciler.tick(T0 + 17 * MINUTE);

        assertEquals(
                1,
                shards.values().stream()
                        .filter(shard -> shard.state() == ShardRecord.State.ACTIVE)
                        .count());
    }

    @Test
    void aLeasedStreamUnplacedForFiveMinutesAlerts() {
        accounts.put("KALSHI", List.of(new Account("KALSHI", "md-1", 0, false)));
        lease("s1", "live", T0, 1);
        sessionStatuses.put("s1", "STARTING");
        reconciler.tick(T0);

        reconciler.tick(T0 + 4 * MINUTE);
        assertFalse(alertEvents.contains("FIRING LEASED_STREAM_UNPLACED:KALSHI:us-east-1"));

        reconciler.tick(T0 + 5 * MINUTE);
        assertTrue(alertEvents.contains("FIRING LEASED_STREAM_UNPLACED:KALSHI:us-east-1"));

        accounts.put("KALSHI", List.of(new Account("KALSHI", "md-1", 0, true)));
        reconciler.tick(T0 + 6 * MINUTE);
        assertTrue(alertEvents.contains("RESOLVED LEASED_STREAM_UNPLACED:KALSHI:us-east-1"));
    }

    @Test
    void aLeaseOnAStreamARuleCouldNotPlaceWaitsItsOwnFiveMinutes() {
        accounts.put("KALSHI", List.of(new Account("KALSHI", "md-1", 0, false)));
        wantStreams(1);
        reconciler.tick(T0);
        lease("s1", "paper", T0 + 20 * MINUTE, 1);
        sessionStatuses.put("s1", "STARTING");

        reconciler.tick(T0 + 24 * MINUTE);

        assertFalse(firingAlerts.containsKey("LEASED_STREAM_UNPLACED:KALSHI:us-east-1"));
    }

    @Test
    void aLeasedStreamThatCanNeverBePlacedAlertsToo() {
        streams.put(4, new StreamInfo(4, "KALSHI", "VENUE", "us-east-1", false));
        lease("s1", "paper", T0, 1, 4, 99);
        sessionStatuses.put("s1", "STARTING");
        reconciler.tick(T0);
        assertNull(targets.get(4), "an ended listing is never a target");

        reconciler.tick(T0 + 5 * MINUTE);

        assertTrue(alertEvents.contains("FIRING LEASED_STREAM_UNPLACED:KALSHI:us-east-1"), "stream 4 ended");
        assertTrue(alertEvents.contains("FIRING LEASED_STREAM_UNPLACED:unknown:unknown"), "the registry lacks 99");
    }

    @Test
    void consolidationNeverMergesBelowMinShards() {
        minShards.put(new Partition("us-east-1", "VENUE", Priority.RESEARCH, "standard"), 2);
        long at = twoUnderFilledShards();

        for (long tick = at + MINUTE; tick <= at + 30 * MINUTE; tick += MINUTE) {
            reconciler.tick(tick);
        }

        assertEquals(0, shards.get("use1-1").mergeableSinceNanos(), "never even mergeable");
        assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-1").state());
        assertEquals(ShardRecord.State.ACTIVE, shards.get("use1-2").state());
    }

    // ----- max_shards -----

    @Test
    void maxShardsLeavesStreamsUnplacedAndAlertsRatherThanStartAShard() {
        maxStreamsPerShard = 1;
        maxShards.put(RESEARCH_US, 2);
        wantStreams(1, 2, 3);

        Reconciler.Summary summary = reconciler.tick(T0);

        assertEquals(2, shards.size());
        assertEquals(1, summary.unplaced());
        assertNull(targets.get(3).shardId());
        assertTrue(alertEvents.contains("FIRING COLLECTION_BUDGET:max_shards:us-east-1:VENUE:RESEARCH:standard"));

        maxShards.put(RESEARCH_US, 3);
        reconciler.tick(T0 + MINUTE);
        assertEquals(3, shards.size());
        assertTrue(alertEvents.contains("RESOLVED COLLECTION_BUDGET:max_shards:us-east-1:VENUE:RESEARCH:standard"));
    }

    @Test
    void aRollMayRunOneShardOverMaxShardsWhileItMovesStreams() {
        maxShards.put(RESEARCH_US, 1);
        wantStreams(1, 2);
        reconciler.tick(T0);
        rollTo = "1.24.0";

        reconciler.tick(T0 + MINUTE);

        assertEquals(ShardRecord.State.DRAINING, shards.get("use1-1").state());
        assertEquals("1.24.0", shards.get("use1-2").version(), "make-before-break: two shards for a moment");
        assertEquals(List.of(1, 2), shards.get("use1-2").streams());

        report("use1-1", "t1", T0 + 2 * MINUTE, "LIVE");
        report("use1-2", "t2", T0 + 2 * MINUTE, "LIVE");
        reconciler.tick(T0 + 2 * MINUTE);
        report("use1-1", "t1", T0 + 4 * MINUTE, "LIVE");
        report("use1-2", "t2", T0 + 4 * MINUTE, "LIVE");
        reconciler.tick(T0 + 4 * MINUTE);
        reconciler.tick(T0 + 8 * MINUTE);
        assertEquals(ShardRecord.State.DELETED, shards.get("use1-1").state());
        assertEquals(Set.of("use1-2@1.24.0"), services, "back to one");
    }

    @Test
    void duringARollAtItsCapANewStreamWaitsRatherThanStartAThirdShard() {
        maxShards.put(RESEARCH_US, 1);
        maxStreamsPerShard = 2;
        wantStreams(1, 2);
        reconciler.tick(T0);
        rollTo = "1.24.0";
        reconciler.tick(T0 + MINUTE);
        streams.put(4, new StreamInfo(4, "KALSHI", "VENUE", "us-east-1", true));
        wantStreams(1, 2, 4);

        Reconciler.Summary summary = reconciler.tick(T0 + 2 * MINUTE);

        assertEquals(2, shards.size());
        assertEquals(1, summary.unplaced());
        assertNull(targets.get(4).shardId());
    }

    @Test
    void minShardsTopsUpOnlyToMaxShards() {
        Partition production = new Partition("us-east-1", "VENUE", Priority.PRODUCTION, "standard");
        minShards.put(production, 3);
        maxShards.put(production, 2);

        reconciler.tick(T0);
        reconciler.tick(T0 + MINUTE);

        assertEquals(2, shards.size());
    }
}
