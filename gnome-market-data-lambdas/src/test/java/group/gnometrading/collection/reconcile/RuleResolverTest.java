package group.gnometrading.collection.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.EventVolume;
import group.gnometrading.collection.model.Interval;
import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.RuleSelector;
import group.gnometrading.collection.model.RuleStatus;
import group.gnometrading.collection.model.Settings;
import group.gnometrading.collection.model.ShardRecord;
import group.gnometrading.collection.model.SourceLimits;
import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Registry;
import group.gnometrading.collection.ports.Store;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RuleResolverTest {

    private static final long MINUTE = TimeUnit.MINUTES.toNanos(1);
    private static final long T0 = 20_000L * TimeUnit.DAYS.toNanos(1);
    private static final Settings SETTINGS = new Settings(
            500,
            5,
            Map.of(),
            "1.23.0",
            null,
            2 * MINUTE,
            10 * MINUTE,
            10 * MINUTE,
            5 * MINUTE,
            5 * MINUTE,
            false,
            false,
            15 * MINUTE,
            0.8);

    // ----- fakes -----

    private final List<Rule> rules = new ArrayList<>();
    private final Map<Integer, Target> targets = new HashMap<>();
    private final Map<String, RuleStatus> statuses = new HashMap<>();
    private int statusWrites;
    // Open events largest volume first, as the registry would rank them, and each event's streams.
    private final List<EventVolume> events = new ArrayList<>();
    private final Map<Integer, Set<Integer>> eventStreams = new HashMap<>();
    private final List<Double> minVolumesAsked = new ArrayList<>();
    private final List<Integer> limitsAsked = new ArrayList<>();
    private RuntimeException registryDown;
    private final Set<Integer> resolvedEvents = new HashSet<>();
    private final Map<String, Set<Integer>> removedEvents = new HashMap<>();

    private final Store store = new Store() {
        @Override
        public List<Rule> activeRules() {
            return List.copyOf(rules);
        }

        @Override
        public void putRuleStatus(final String ruleId, final RuleStatus status) {
            statuses.put(ruleId, status);
            statusWrites++;
        }

        @Override
        public void removeEventIds(final String ruleId, final Set<Integer> eventIds) {
            removedEvents.computeIfAbsent(ruleId, id -> new HashSet<>()).addAll(eventIds);
        }

        @Override
        public Map<Integer, Target> targets() {
            return new HashMap<>(targets);
        }

        @Override
        public void putTarget(final Target target) {}

        @Override
        public void deleteTarget(final int streamId) {}

        @Override
        public List<ShardRecord> shards() {
            return List.of();
        }

        @Override
        public boolean putShard(final ShardRecord shard, final long expectedGeneration) {
            return true;
        }

        @Override
        public List<Interval> openIntervals(final String shardId) {
            return List.of();
        }

        @Override
        public void putInterval(final Interval interval) {}

        @Override
        public void putGap(final int streamId, final long fromNanos, final long toNanos, final String reason) {}

        @Override
        public Map<String, String> firingAlerts() {
            return Map.of();
        }

        @Override
        public void putFiringAlerts(final Map<String, String> typeByKey) {}
    };

    private final Registry registry = new Registry() {
        @Override
        public Map<Integer, StreamInfo> streams(final Collection<Integer> streamIds) {
            return Map.of();
        }

        @Override
        public Map<String, SourceLimits> sources() {
            return Map.of();
        }

        @Override
        public Map<String, List<Account>> accounts() {
            return Map.of();
        }

        @Override
        public List<EventVolume> events(
                final RuleSelector.EventFilter filter,
                final boolean byVolume,
                final double minVolume24h,
                final long expiresBeforeMillis,
                final int limit) {
            failIfDown();
            minVolumesAsked.add(minVolume24h);
            limitsAsked.add(limit);
            return events.stream()
                    .filter(e -> e.volume24h() >= minVolume24h)
                    .limit(limit)
                    .toList();
        }

        @Override
        public Set<Integer> resolvedEvents(final Collection<Integer> eventIds) {
            failIfDown();
            return eventIds.stream().filter(resolvedEvents::contains).collect(java.util.stream.Collectors.toSet());
        }

        @Override
        public Map<Integer, Set<Integer>> streamsOfEvents(final Collection<Integer> eventIds) {
            failIfDown();
            final Map<Integer, Set<Integer>> byEvent = new HashMap<>();
            eventIds.stream().filter(eventStreams::containsKey).forEach(id -> byEvent.put(id, eventStreams.get(id)));
            return byEvent;
        }

        private void failIfDown() {
            if (registryDown != null) {
                throw registryDown;
            }
        }
    };

    private RuleResolver resolver = new RuleResolver(store, registry);

    // ----- helpers -----

    private void rule(final RuleSelector selector) {
        rules.clear();
        rules.add(new Rule("r1", selector, Set.of(), Priority.RESEARCH, "standard", null));
    }

    private static RuleSelector.EventFilter topN(final int n, final int maxStreams) {
        return new RuleSelector.EventFilter(List.of(1), List.of(), List.of(), null, null, n, maxStreams);
    }

    private static RuleSelector.EventFilter minVolume(final double min) {
        return new RuleSelector.EventFilter(List.of(1), List.of(), List.of(), null, min, null, 100);
    }

    /** Events 1..n, two streams each (event e: 10e+1 and 10e+2), with these volumes, ranked largest first. */
    private void events(final double... volumes) {
        events.clear();
        for (int e = 1; e <= volumes.length; e++) {
            events.add(new EventVolume(e, volumes[e - 1]));
            eventStreams.put(e, Set.of(10 * e + 1, 10 * e + 2));
        }
        events.sort((a, b) -> Double.compare(b.volume24h(), a.volume24h()));
    }

    /** Resolves at {@code now} and keeps what the rule selected as its desired targets, as evaluation would. */
    private Set<Integer> resolve(final long now) {
        final Tick tick = new Tick(store, SETTINGS, now);
        resolver.run(tick);
        final Set<Integer> selected = tick.rules.get(0).streamIds();
        targets.clear();
        selected.forEach(id -> targets.put(
                id,
                Target.desired(
                        id,
                        "KALSHI",
                        new Partition("us-east-1", "VENUE", Priority.RESEARCH, "standard"),
                        Set.of("r1"),
                        null,
                        now)));
        return selected;
    }

    private static Set<Integer> streamsOf(final int... events) {
        final Set<Integer> streams = new HashSet<>();
        for (int e : events) {
            streams.add(10 * e + 1);
            streams.add(10 * e + 2);
        }
        return streams;
    }

    // ----- tests -----

    @Test
    void aStaticRuleKeepsItsStreamsAndAsksNothing() {
        rules.add(new Rule("r1", Set.of(1, 2), Priority.RESEARCH, "standard", null));
        final Tick tick = new Tick(store, SETTINGS, T0);
        resolver.run(tick);
        assertEquals(Set.of(1, 2), tick.rules.get(0).streamIds());
        assertEquals(0, statusWrites);
    }

    @Test
    void anEventRuleTakesEveryStreamOfItsEventsAndAContractAddedLaterOnTheNextResolve() {
        eventStreams.put(7, Set.of(71, 72));
        rule(new RuleSelector.EventIds(Set.of(7, 8)));
        assertEquals(Set.of(71, 72), resolve(T0));
        assertEquals(new RuleStatus(1, 2, 1, false, T0), statuses.get("r1"), "8 has no stream yet");

        eventStreams.put(7, Set.of(71, 72, 73, 74));
        assertEquals(Set.of(71, 72), resolve(T0 + MINUTE), "resolved again only every few minutes");
        assertEquals(Set.of(71, 72, 73, 74), resolve(T0 + RuleResolver.RESOLVE_EVERY_NANOS));
    }

    @Test
    void aResolvedEventIsTakenOutOfItsRuleAndSelectsNothing() {
        eventStreams.put(7, Set.of(71, 72));
        eventStreams.put(8, Set.of(81));
        resolvedEvents.add(8);
        rule(new RuleSelector.EventIds(Set.of(7, 8)));

        assertEquals(Set.of(71, 72), resolve(T0));
        assertEquals(Map.of("r1", Set.of(8)), removedEvents);
        assertEquals(new RuleStatus(1, 2, 0, false, T0), statuses.get("r1"));
    }

    @Test
    void anEventRuleWithNothingResolvedRemovesNothing() {
        eventStreams.put(7, Set.of(71));
        rule(new RuleSelector.EventIds(Set.of(7)));
        resolve(T0);
        assertTrue(removedEvents.isEmpty());
    }

    @Test
    void aTopNRuleKeepsAnEventThatSlipsWithinTheBandAndDropsOneBelowIt() {
        events(100, 90, 80, 70);
        rule(topN(2, 100));
        assertEquals(streamsOf(1, 2), resolve(T0));
        assertEquals(List.of(3), limitsAsked, "asks for the band: 1.5 times top N");

        events(100, 85, 95, 70);
        assertEquals(streamsOf(1, 2, 3), resolve(T0 + 5 * MINUTE), "2 is third, still within 3; 3 joins as second");

        events(100, 60, 95, 90);
        assertEquals(streamsOf(1, 3), resolve(T0 + 10 * MINUTE), "2 ranks fourth, outside the band");
    }

    @Test
    void anEventOutsideTopNDoesNotJoinEvenWithinTheBand() {
        events(100, 90, 80);
        rule(topN(2, 100));
        assertEquals(streamsOf(1, 2), resolve(T0));
    }

    @Test
    void aMinimumVolumeRuleKeepsAnEventDownToHalfTheMinimum() {
        events(1200, 800);
        rule(minVolume(1000));
        assertEquals(streamsOf(1), resolve(T0));
        assertEquals(List.of(500.0), minVolumesAsked, "asks down to half the minimum, for events it keeps");

        events(600, 800);
        assertEquals(streamsOf(1), resolve(T0 + 5 * MINUTE), "600 is above half; 800 never reached 1000");

        events(400, 800);
        assertEquals(Set.of(), resolve(T0 + 10 * MINUTE));
    }

    @Test
    void maxStreamsStopsAtTheLastWholeEventThatFitsAndTheRuleShowsCapped() {
        events(100, 90, 80);
        rule(topN(3, 5));
        assertEquals(streamsOf(1, 2), resolve(T0));
        assertEquals(new RuleStatus(2, 4, 0, true, T0), statuses.get("r1"));
    }

    @Test
    void changingARulesParametersResolvesItAtOnce() {
        events(100, 90, 80);
        rule(topN(1, 100));
        assertEquals(streamsOf(1), resolve(T0));

        rule(topN(2, 100));
        assertEquals(streamsOf(1, 2), resolve(T0 + MINUTE));
    }

    @Test
    void aStatusIsWrittenOnlyWhenTheSelectionChanges() {
        events(100, 90);
        rule(topN(2, 100));
        resolve(T0);
        resolve(T0 + 5 * MINUTE);
        assertEquals(1, statusWrites);

        events(100, 90, 80);
        rule(topN(3, 100));
        resolve(T0 + 6 * MINUTE);
        assertEquals(2, statusWrites);
    }

    @Test
    void aRegistryFailureKeepsTheLastSelectionAndFailsARuleNeverResolved() {
        events(100, 90);
        rule(topN(2, 100));
        resolve(T0);

        registryDown = new IllegalStateException("registry down");
        assertEquals(streamsOf(1, 2), resolve(T0 + 5 * MINUTE));

        resolver = new RuleResolver(store, registry);
        assertThrows(IllegalStateException.class, () -> resolve(T0 + 6 * MINUTE));
    }

    @Test
    void aRestartedReconcilerStillKnowsWhichEventsItCollects() {
        events(100, 90, 80);
        rule(topN(2, 100));
        resolve(T0);

        resolver = new RuleResolver(store, registry);
        events(100, 85, 95);
        assertTrue(resolve(T0 + MINUTE).containsAll(streamsOf(2)), "2 is held by its targets, so it stays in the band");
    }
}
