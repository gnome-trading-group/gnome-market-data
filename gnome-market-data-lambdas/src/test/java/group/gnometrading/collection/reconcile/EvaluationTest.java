package group.gnometrading.collection.reconcile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.model.Target;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class EvaluationTest {

    private static final long MINUTE = TimeUnit.MINUTES.toNanos(1);
    private static final long NOW = 1_000 * MINUTE;

    private final Evaluation evaluation = new Evaluation(5, Map.of("HYPERLIQUID", 0));
    private final Map<Integer, StreamInfo> streams = new HashMap<>(Map.of(
            1, new StreamInfo(1, "KALSHI", "VENUE", "us-east-1", true),
            2, new StreamInfo(2, "HYPERLIQUID", "VENUE", "ap-northeast-1", true)));
    private final Map<Integer, Target> targets = new HashMap<>();

    private static Rule rule(final String id, final Priority priority, final Integer grace, final Integer... streams) {
        return new Rule(id, Set.of(streams), priority, "standard", grace);
    }

    private List<Target> evaluate(final List<Rule> rules, final long now) {
        List<Target> changed = evaluation.evaluate(rules, targets, streams, now).changed();
        for (Target target : changed) {
            targets.put(target.streamId(), target);
        }
        return changed;
    }

    @Test
    void aWantedStreamBecomesADesiredTargetUncoveredUntilPlaced() {
        Target target =
                evaluate(List.of(rule("r1", Priority.RESEARCH, null, 1)), NOW).get(0);

        assertEquals(Target.State.DESIRED, target.state());
        assertEquals(new Partition("us-east-1", "VENUE", Priority.RESEARCH, "standard"), target.partition());
        assertEquals(Set.of("r1"), target.wantedBy());
        assertEquals(NOW, target.uncoveredSinceNanos());
        assertEquals(Target.NOT_PLACED, target.uncoveredReason());
    }

    @Test
    void theHighestPriorityRuleDecidesThePartition() {
        Target target = evaluate(
                        List.of(rule("r1", Priority.RESEARCH, null, 1), rule("lease", Priority.PRODUCTION, null, 1)),
                        NOW)
                .get(0);

        assertEquals(Priority.PRODUCTION, target.partition().priority());
        assertEquals(Set.of("r1", "lease"), target.wantedBy());
    }

    @Test
    void anUnwantedStreamRetiresAfterItsRulesGrace() {
        evaluate(List.of(rule("r1", Priority.RESEARCH, 2, 1)), NOW);

        Target retiring = evaluate(List.of(), NOW).get(0);
        assertEquals(Target.State.RETIRING, retiring.state());
        assertEquals(NOW + 2 * MINUTE, retiring.retireAfterNanos());

        assertTrue(evaluate(List.of(), NOW + MINUTE).isEmpty(), "still in its grace");
        assertEquals(
                Target.State.RETIRED,
                evaluate(List.of(), NOW + 2 * MINUTE).get(0).state());
    }

    @Test
    void anEndedSubjectRetiresAfterItsSourcesGrace() {
        evaluate(List.of(rule("r1", Priority.RESEARCH, null, 1, 2)), NOW);
        streams.put(1, new StreamInfo(1, "KALSHI", "VENUE", "us-east-1", false));
        streams.put(2, new StreamInfo(2, "HYPERLIQUID", "VENUE", "ap-northeast-1", false));

        List<Target> retiring = evaluate(List.of(rule("r1", Priority.RESEARCH, null, 1, 2)), NOW);

        assertEquals(NOW + 5 * MINUTE, retiring.get(0).retireAfterNanos(), "Kalshi keeps the default");
        assertEquals(NOW, retiring.get(1).retireAfterNanos(), "Hyperliquid retires at once");
    }

    @Test
    void wantedAgainWhileRetiringItIsDesiredOnTheSameShard() {
        evaluate(List.of(rule("r1", Priority.RESEARCH, null, 1)), NOW);
        targets.put(1, targets.get(1).onShard("use1-a"));
        evaluate(List.of(), NOW);

        Target back = evaluate(List.of(rule("r2", Priority.RESEARCH, null, 1)), NOW + MINUTE)
                .get(0);
        assertEquals(Target.State.DESIRED, back.state());
        assertEquals("use1-a", back.shardId());
        assertEquals(Set.of("r2"), back.wantedBy());
    }

    @Test
    void aStreamNotInTheRegistryIsNeverDesired() {
        assertTrue(
                evaluate(List.of(rule("r1", Priority.RESEARCH, null, 99)), NOW).isEmpty());
    }

    @Test
    void nothingChangedMeansNothingWritten() {
        evaluate(List.of(rule("r1", Priority.RESEARCH, null, 1)), NOW);

        assertTrue(evaluate(List.of(rule("r1", Priority.RESEARCH, null, 1)), NOW + MINUTE)
                .isEmpty());
    }

    @Test
    void aLeasesShortGraceCountsOnlyWhenNoRuleWantsTheStream() {
        Rule lease = rule("lease:s1", Priority.RESEARCH, 5, 1);

        assertEquals(
                30,
                evaluate(List.of(lease, rule("r1", Priority.RESEARCH, 30, 1)), NOW)
                        .get(0)
                        .retireGraceMinutes(),
                "the rule's longer grace");
        targets.clear();
        assertNull(
                evaluate(List.of(lease, rule("r1", Priority.RESEARCH, null, 1)), NOW)
                        .get(0)
                        .retireGraceMinutes(),
                "the rule's default, not the lease's 5");
        targets.clear();
        assertEquals(5, evaluate(List.of(lease), NOW).get(0).retireGraceMinutes(), "a lease alone keeps its own");
    }

    @Test
    void aStreamOnlyALeaseWantedRetiresAfterTheLeasesGrace() {
        evaluate(List.of(rule("lease:s1", Priority.RESEARCH, 5, 1)), NOW);

        Target retiring = evaluate(List.of(), NOW + MINUTE).get(0);

        assertEquals(Target.State.RETIRING, retiring.state());
        assertEquals(NOW + 6 * MINUTE, retiring.retireAfterNanos());
    }
}
