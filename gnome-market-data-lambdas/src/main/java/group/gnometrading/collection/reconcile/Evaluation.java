package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.model.Target;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Rules to targets (the reconciler's evaluate step). A stream some active rule wants, whose subject hasn't ended, is
 * {@link Target.State#DESIRED}, at the highest priority any of them asks for. One nobody wants any more, or whose
 * subject ended (its listing inactive), goes {@link Target.State#RETIRING} for a grace period, then
 * {@link Target.State#RETIRED}: the reconciler takes it off its shard and deletes it. Wanted again while retiring, it is
 * desired again, never having left its shard.
 *
 * <p>The grace lets an ended market's last messages (a settlement) arrive. It is the source's when the subject ended
 * ({@code endedGraceMinutes}, e.g. 0 for Hyperliquid, which drops a connection holding both sides of an expired outcome
 * market), else the rule's, else {@code defaultGraceMinutes}.
 */
public final class Evaluation {

    /** Targets created or changed; retired ones are among them, for the reconciler to take off their shards. */
    public record Result(List<Target> changed) {}

    private final int defaultGraceMinutes;
    private final Map<String, Integer> endedGraceMinutes;

    public Evaluation(final int defaultGraceMinutes, final Map<String, Integer> endedGraceMinutes) {
        this.defaultGraceMinutes = defaultGraceMinutes;
        this.endedGraceMinutes = endedGraceMinutes;
    }

    public Result evaluate(
            final List<Rule> rules,
            final Map<Integer, Target> targets,
            final Map<Integer, StreamInfo> streams,
            final long nowNanos) {
        final Map<Integer, List<Rule>> wanting = new HashMap<>();
        for (Rule rule : rules) {
            for (int streamId : rule.streamIds()) {
                wanting.computeIfAbsent(streamId, id -> new ArrayList<>()).add(rule);
            }
        }
        final Set<Integer> all = new HashSet<>(wanting.keySet());
        all.addAll(targets.keySet());
        final List<Target> changed = new ArrayList<>();
        for (int streamId : new TreeSet<>(all)) {
            final Target updated = evaluate(
                    streamId,
                    targets.get(streamId),
                    wanting.getOrDefault(streamId, List.of()),
                    streams.get(streamId),
                    nowNanos);
            if (updated != null && !updated.equals(targets.get(streamId))) {
                changed.add(updated);
            }
        }
        return new Result(changed);
    }

    private Target evaluate(
            final int streamId, final Target current, final List<Rule> rules, final StreamInfo stream, final long now) {
        final boolean ended = stream == null || !stream.live();
        if (!rules.isEmpty() && !ended) {
            return wanted(streamId, current, rules, stream, now);
        }
        return current == null ? null : unwanted(current, ended, now);
    }

    private static Target wanted(
            final int streamId, final Target current, final List<Rule> rules, final StreamInfo stream, final long now) {
        final Rule first = highestPriority(rules);
        final Set<String> wantedBy = new TreeSet<>();
        Integer grace = null;
        for (Rule rule : rules) {
            wantedBy.add(rule.ruleId());
            if (rule.retireGraceMinutes() != null) {
                grace = grace == null ? rule.retireGraceMinutes() : Math.max(grace, rule.retireGraceMinutes());
            }
        }
        final Partition partition = new Partition(stream.region(), stream.sourceKind(), first.priority(), first.tier());
        if (current == null) {
            return Target.desired(streamId, stream.source(), partition, wantedBy, grace, now);
        }
        return current.desired(partition, wantedBy, grace);
    }

    private Target unwanted(final Target current, final boolean ended, final long now) {
        if (current.state() == Target.State.DESIRED) {
            return current.retiring(now + TimeUnit.MINUTES.toNanos(graceMinutes(current, ended)));
        }
        if (current.state() == Target.State.RETIRING && now >= current.retireAfterNanos()) {
            return current.retired();
        }
        return current;
    }

    private int graceMinutes(final Target target, final boolean ended) {
        if (ended) {
            return endedGraceMinutes.getOrDefault(target.source(), defaultGraceMinutes);
        }
        return target.retireGraceMinutes() == null ? defaultGraceMinutes : target.retireGraceMinutes();
    }

    /** The rule asking for the highest priority; its tier is the target's. */
    private static Rule highestPriority(final List<Rule> rules) {
        Rule best = rules.get(0);
        for (Rule rule : rules) {
            if (rule.priority().ordinal() < best.priority().ordinal()) {
                best = rule;
            }
        }
        return best;
    }
}
