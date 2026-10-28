package group.gnometrading.collection.aws;

import group.gnometrading.SecurityMaster;
import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.EventVolume;
import group.gnometrading.collection.model.RuleSelector;
import group.gnometrading.collection.model.SourceLimits;
import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.ports.Registry;
import group.gnometrading.md.Source;
import group.gnometrading.md.SourceAccount;
import group.gnometrading.md.Stream;
import group.gnometrading.sm.Event;
import group.gnometrading.sm.EventQuery;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The registry as the reconciler reads it: streams in bulk ({@code active} following each listing, so an ended market
 * reads as not live; the reconciler keeps these a few minutes, see {@code TargetsStep}), every source's capacity and
 * every market-data account, the last two each tick, and the events and event streams rules select (every few minutes
 * per rule, see {@code RuleResolver}).
 */
public final class RegistryAdapter implements Registry {

    /**
     * The sources whose feeds sign in with a market-data account: gnome-orchestrator's {@code FeedVenues} marks these
     * signed, and a shard binds one account each. The registry doesn't record it, so the two lists must agree.
     */
    static final Set<String> SIGNED_SOURCES = Set.of("KALSHI", "POLYMARKET_US");

    private final SecurityMaster securityMaster;
    private Map<String, Source> sources = Map.of();

    public RegistryAdapter(final SecurityMaster securityMaster) {
        this.securityMaster = securityMaster;
    }

    @Override
    public Map<Integer, StreamInfo> streams(final Collection<Integer> streamIds) {
        final Map<String, Source> byCode = sourcesByCode();
        final Map<Integer, StreamInfo> streams = new HashMap<>();
        final int[] ids = streamIds.stream().mapToInt(Integer::intValue).toArray();
        for (Stream stream : securityMaster.getStreams(ids)) {
            final Source source = byCode.get(stream.sourceCode());
            if (source == null) {
                continue;
            }
            streams.put(
                    stream.streamId(),
                    new StreamInfo(
                            stream.streamId(),
                            stream.sourceCode(),
                            source.kind().name(),
                            stream.resolvedRegion(),
                            stream.active() && source.active()));
        }
        return streams;
    }

    @Override
    public List<EventVolume> events(
            final RuleSelector.EventFilter filter,
            final boolean byVolume,
            final double minVolume24h,
            final long expiresBeforeMillis,
            final int limit) {
        final EventQuery query = new EventQuery(
                filter.exchangeIds(),
                filter.categories(),
                filter.tags(),
                expiresBeforeMillis,
                minVolume24h,
                byVolume ? EventQuery.Order.VOLUME : EventQuery.Order.EXPIRY,
                limit);
        final List<EventVolume> events = new ArrayList<>();
        for (Event event : securityMaster.getEvents(query)) {
            events.add(new EventVolume(event.eventId(), event.volume24h()));
        }
        return events;
    }

    @Override
    public Set<Integer> resolvedEvents(final Collection<Integer> eventIds) {
        final Set<Integer> resolved = new HashSet<>();
        for (Event event : securityMaster.getEventsByIds(
                eventIds.stream().mapToInt(Integer::intValue).toArray())) {
            if (event.resolved()) {
                resolved.add(event.eventId());
            }
        }
        return resolved;
    }

    @Override
    public Map<Integer, Set<Integer>> streamsOfEvents(final Collection<Integer> eventIds) {
        final Map<Integer, Set<Integer>> streams = new HashMap<>();
        final int[] ids = eventIds.stream().mapToInt(Integer::intValue).toArray();
        for (Stream stream : securityMaster.getStreamsForEvents(ids)) {
            if (stream.active()) {
                streams.computeIfAbsent(stream.eventId(), event -> new HashSet<>())
                        .add(stream.streamId());
            }
        }
        return streams;
    }

    @Override
    public Map<String, SourceLimits> sources() {
        final Map<String, SourceLimits> limits = new HashMap<>();
        for (Source source : sourcesByCode().values()) {
            limits.put(source.code(), limitsOf(source));
        }
        return limits;
    }

    @Override
    public Map<String, List<Account>> accounts() {
        final Map<String, List<Account>> accounts = new HashMap<>();
        for (SourceAccount account : securityMaster.getSourceAccounts()) {
            accounts.computeIfAbsent(account.sourceCode(), code -> new ArrayList<>())
                    .add(new Account(
                            account.sourceCode(), account.label(), account.connectionBudget(), account.active()));
        }
        return accounts;
    }

    static SourceLimits limitsOf(final Source source) {
        return new SourceLimits(
                source.code(),
                source.active() ? source.maxPerConnection() : 0,
                SourceLimits.Scope.valueOf(source.connectionScope().toUpperCase(Locale.ROOT)),
                source.connectionsPerScope(),
                SIGNED_SOURCES.contains(source.code()));
    }

    /** Fetched once per adapter, which a tick builds afresh, so a tick sees one consistent set. */
    private Map<String, Source> sourcesByCode() {
        if (sources.isEmpty()) {
            final Map<String, Source> byCode = new HashMap<>();
            for (Source source : securityMaster.getSources()) {
                byCode.put(source.code(), source);
            }
            sources = Map.copyOf(byCode);
        }
        return sources;
    }
}
