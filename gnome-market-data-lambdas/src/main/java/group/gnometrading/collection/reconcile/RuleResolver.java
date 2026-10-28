package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.EventVolume;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.RuleSelector;
import group.gnometrading.collection.model.RuleStatus;
import group.gnometrading.collection.model.Target;
import group.gnometrading.collection.ports.Registry;
import group.gnometrading.collection.ports.Store;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Reads the active rules and fills in the streams each selects now, so {@link Evaluation} sees every rule as a set of
 * streams. A static rule names its streams; an {@code event} rule takes every active stream of its events; an
 * {@code event_filter} rule asks the registry for matching open events and takes theirs.
 *
 * <p>An event rule is resolved again every {@link #RESOLVE_EVERY_NANOS}, or at once when its parameters change: events
 * and their volumes move slowly, and resolving is several registry calls. If the registry fails, the rule keeps what it
 * last resolved to; a rule never resolved fails the tick, which the next minute's retries.
 *
 * <p>A volume rule's band needs no state of its own: an event is already collected when any of its streams is a
 * target this rule wants, which survives restarts of the Lambda.
 */
final class RuleResolver {

    private static final Logger LOGGER = LogManager.getLogger(RuleResolver.class);

    static final long RESOLVE_EVERY_NANOS = TimeUnit.MINUTES.toNanos(5);
    // An event already collected stays while it ranks within RANK_BAND times top_n and keeps VOLUME_BAND of the minimum
    // volume, so one hovering at the edge doesn't join and leave every few minutes.
    static final double RANK_BAND = 1.5;
    static final double VOLUME_BAND = 0.5;

    private record Resolved(RuleSelector selector, Set<Integer> streams, RuleStatus status, long atNanos) {}

    private final Store store;
    private final Registry registry;
    // Lives as long as the Lambda's container: a cold start resolves every rule once.
    private final Map<String, Resolved> resolved = new HashMap<>();

    RuleResolver(final Store store, final Registry registry) {
        this.store = store;
        this.registry = registry;
    }

    void run(final Tick tick) {
        final List<Rule> rules = new ArrayList<>();
        final Set<String> ruleIds = new HashSet<>();
        for (Rule rule : store.activeRules()) {
            ruleIds.add(rule.ruleId());
            rules.add(
                    rule.selector() instanceof RuleSelector.StaticStreams
                            ? rule
                            : rule.withStreams(streams(tick, rule)));
        }
        resolved.keySet().retainAll(ruleIds);
        tick.rules = rules;
    }

    private Set<Integer> streams(final Tick tick, final Rule rule) {
        final Resolved last = resolved.get(rule.ruleId());
        if (last != null
                && last.selector().equals(rule.selector())
                && tick.now - last.atNanos() < RESOLVE_EVERY_NANOS) {
            return last.streams();
        }
        final Resolved fresh;
        try {
            fresh = resolve(tick, rule);
        } catch (RuntimeException error) {
            if (last == null) {
                throw error;
            }
            LOGGER.warn(
                    "Keeping rule {}'s last {} streams: resolving failed: {}",
                    rule.ruleId(),
                    last.streams().size(),
                    error.toString());
            return last.streams();
        }
        resolved.put(rule.ruleId(), fresh);
        if (last == null || !fresh.status().sameSelection(last.status())) {
            store.putRuleStatus(rule.ruleId(), fresh.status());
        }
        return fresh.streams();
    }

    private Resolved resolve(final Tick tick, final Rule rule) {
        if (rule.selector() instanceof RuleSelector.EventIds events) {
            return resolveEvents(tick, rule, events);
        }
        return resolveFilter(tick, rule, (RuleSelector.EventFilter) rule.selector());
    }

    /**
     * An {@code event} rule's streams. Resolved events come out of the rule: their listings already went inactive, so
     * they select nothing, and leaving them would grow the rule with every event a pushing system ever added.
     */
    private Resolved resolveEvents(final Tick tick, final Rule rule, final RuleSelector.EventIds events) {
        final Set<Integer> open = new HashSet<>(events.eventIds());
        final Set<Integer> resolvedEvents = registry.resolvedEvents(events.eventIds());
        if (!resolvedEvents.isEmpty()) {
            store.removeEventIds(rule.ruleId(), resolvedEvents);
            open.removeAll(resolvedEvents);
            LOGGER.info("Removed {} resolved events from rule {}", resolvedEvents.size(), rule.ruleId());
        }
        final Map<Integer, Set<Integer>> byEvent = registry.streamsOfEvents(open);
        final Set<Integer> streams = new HashSet<>();
        byEvent.values().forEach(streams::addAll);
        final RuleStatus status =
                new RuleStatus(byEvent.size(), streams.size(), open.size() - byEvent.size(), false, tick.now);
        return new Resolved(rule.selector(), streams, status, tick.now);
    }

    private Resolved resolveFilter(final Tick tick, final Rule rule, final RuleSelector.EventFilter filter) {
        final int limit = filter.topN() == null
                ? filter.maxStreams()
                : Math.min((int) Math.ceil(RANK_BAND * filter.topN()), filter.maxStreams());
        final double minVolume = filter.minVolume24h() == null ? 0 : filter.minVolume24h() * VOLUME_BAND;
        final long expiresBefore = filter.expiresWithinHours() == null
                ? 0
                : TimeUnit.NANOSECONDS.toMillis(tick.now) + TimeUnit.HOURS.toMillis(filter.expiresWithinHours());
        final List<EventVolume> ranked = registry.events(filter, filter.byVolume(), minVolume, expiresBefore, limit);
        final Map<Integer, Set<Integer>> byEvent = registry.streamsOfEvents(
                ranked.stream().map(EventVolume::eventId).toList());
        final Set<Integer> held = wantedBy(tick, rule.ruleId());

        final Set<Integer> streams = new HashSet<>();
        int events = 0;
        boolean capped = false;
        for (int rank = 0; rank < ranked.size(); rank++) {
            final EventVolume event = ranked.get(rank);
            final Set<Integer> eventStreams = byEvent.getOrDefault(event.eventId(), Set.of());
            if (eventStreams.isEmpty()) {
                continue;
            }
            final boolean collected = !Collections.disjoint(eventStreams, held);
            if (!(collected ? stays(filter, rank, event) : joins(filter, rank, event))) {
                continue;
            }
            if (streams.size() + eventStreams.size() > filter.maxStreams()) {
                capped = true;
                break;
            }
            streams.addAll(eventStreams);
            events++;
        }
        return new Resolved(
                rule.selector(), streams, new RuleStatus(events, streams.size(), 0, capped, tick.now), tick.now);
    }

    private static boolean joins(final RuleSelector.EventFilter filter, final int rank, final EventVolume event) {
        return (filter.topN() == null || rank < filter.topN())
                && (filter.minVolume24h() == null || event.volume24h() >= filter.minVolume24h());
    }

    private static boolean stays(final RuleSelector.EventFilter filter, final int rank, final EventVolume event) {
        return (filter.topN() == null || rank < RANK_BAND * filter.topN())
                && (filter.minVolume24h() == null || event.volume24h() >= VOLUME_BAND * filter.minVolume24h());
    }

    /** The streams a rule wants now: its desired targets. */
    private static Set<Integer> wantedBy(final Tick tick, final String ruleId) {
        final Set<Integer> streams = new HashSet<>();
        for (Target target : tick.targets.values()) {
            if (target.state() == Target.State.DESIRED && target.wantedBy().contains(ruleId)) {
                streams.add(target.streamId());
            }
        }
        return streams;
    }
}
