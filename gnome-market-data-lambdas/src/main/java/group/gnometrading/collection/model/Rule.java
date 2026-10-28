package group.gnometrading.collection.model;

import java.util.Set;

/**
 * A rule as evaluation reads it: what it selects and the streams that selects now. A static rule's streams are its
 * own; the others' are filled in each tick by {@code RuleResolver}.
 */
public record Rule(
        String ruleId,
        RuleSelector selector,
        Set<Integer> streamIds,
        Priority priority,
        String tier,
        Integer retireGraceMinutes) {

    /** A {@code static_streams} rule. */
    public Rule(
            final String ruleId,
            final Set<Integer> streamIds,
            final Priority priority,
            final String tier,
            final Integer retireGraceMinutes) {
        this(
                ruleId,
                new RuleSelector.StaticStreams(Set.copyOf(streamIds)),
                Set.copyOf(streamIds),
                priority,
                tier,
                retireGraceMinutes);
    }

    public Rule withStreams(final Set<Integer> streams) {
        return new Rule(ruleId, selector, Set.copyOf(streams), priority, tier, retireGraceMinutes);
    }
}
