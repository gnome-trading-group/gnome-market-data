package group.gnometrading.collection.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.RegistryConnection;
import group.gnometrading.SecurityMaster;
import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.EventVolume;
import group.gnometrading.collection.model.ListingVolume;
import group.gnometrading.collection.model.RuleSelector;
import group.gnometrading.collection.model.SourceLimits;
import group.gnometrading.collection.model.StreamInfo;
import group.gnometrading.collection.ports.Registry;
import group.gnometrading.md.Source;
import group.gnometrading.md.SourceAccount;
import group.gnometrading.md.Stream;
import group.gnometrading.sm.Event;
import group.gnometrading.sm.EventQuery;
import group.gnometrading.sm.ListingQuery;
import group.gnometrading.sm.PropositionFilter;
import group.gnometrading.sm.SecurityType;
import group.gnometrading.strings.ViewString;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
 * every market-data account, the last two each tick, the events and event streams rules select (every few minutes
 * per rule, see {@code RuleResolver}), and each tick every session not yet ended, for the leases.
 */
public final class RegistryAdapter implements Registry {

    /**
     * The sources whose feeds sign in with a market-data account: gnome-orchestrator's {@code FeedVenues} marks these
     * signed, and a shard binds one account each. The registry doesn't record it, so the two lists must agree.
     */
    static final Set<String> SIGNED_SOURCES = Set.of("KALSHI", "POLYMARKET_US");

    // Every status before STOPPED/FAILED. The launcher writes a session's row before its lease, so a lease holder
    // missing
    // from these has ended.
    static final String OPEN_SESSIONS_PATH = "/api/strategy-sessions?status=SUBMITTED,STARTING,RUNNING,STOPPING";

    private final SecurityMaster securityMaster;
    private final RegistryConnection connection;
    private final ObjectMapper mapper;
    private Map<String, Source> sources = Map.of();

    public RegistryAdapter(
            final SecurityMaster securityMaster, final RegistryConnection connection, final ObjectMapper mapper) {
        this.securityMaster = securityMaster;
        this.connection = connection;
        this.mapper = mapper;
    }

    /** One request a tick, however many leases there are. */
    @Override
    public Map<String, String> openSessionStatuses(final String mode) {
        final String path = mode == null
                ? OPEN_SESSIONS_PATH
                : OPEN_SESSIONS_PATH + "&mode=" + URLEncoder.encode(mode, StandardCharsets.UTF_8);
        final ByteBuffer body = connection.get(new ViewString(path));
        final byte[] bytes = new byte[body.remaining()];
        body.get(bytes);
        final JsonNode rows;
        try {
            rows = mapper.readTree(bytes);
        } catch (IOException error) {
            throw new IllegalStateException("Unreadable sessions from the registry", error);
        }
        if (rows == null || !rows.isArray()) {
            throw new IllegalStateException("The registry's sessions aren't a list: " + rows);
        }
        final Map<String, String> statuses = new HashMap<>();
        for (JsonNode row : rows) {
            if (row.hasNonNull("session_id") && row.hasNonNull("status")) {
                statuses.put(row.get("session_id").asText(), row.get("status").asText());
            }
        }
        return statuses;
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
            final long startsBeforeMillis,
            final int limit) {
        final EventQuery query = new EventQuery(
                filter.exchangeIds(),
                filter.categories(),
                filter.tags(),
                propositions(filter),
                expiresBeforeMillis,
                startsBeforeMillis,
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
    public Map<Integer, Set<Integer>> streamsOfEvents(
            final Collection<Integer> eventIds, final RuleSelector.EventFilter filter) {
        final Map<Integer, Set<Integer>> streams = new HashMap<>();
        final int[] ids = eventIds.stream().mapToInt(Integer::intValue).toArray();
        for (Stream stream : securityMaster.getStreamsForEvents(ids, filter == null ? null : propositions(filter))) {
            if (stream.active()) {
                streams.computeIfAbsent(stream.eventId(), event -> new HashSet<>())
                        .add(stream.streamId());
            }
        }
        return streams;
    }

    @Override
    public List<ListingVolume> listings(
            final RuleSelector.ListingFilter filter, final double minVolume24h, final int limit) {
        final ListingQuery query = new ListingQuery(
                filter.exchangeIds(),
                filter.securityTypes().stream().map(SecurityType::valueOf).toList(),
                minVolume24h,
                limit);
        final List<ListingVolume> listings = new ArrayList<>();
        for (group.gnometrading.sm.ListingVolume listing : securityMaster.getListingVolumes(query)) {
            listings.add(new ListingVolume(listing.listingId(), listing.volume24h()));
        }
        return listings;
    }

    @Override
    public Map<Integer, Integer> streamsOfListings(final Collection<Integer> listingIds) {
        final Map<Integer, Integer> streams = new HashMap<>();
        final int[] ids = listingIds.stream().mapToInt(Integer::intValue).toArray();
        for (Stream stream : securityMaster.getStreamsForListings(ids)) {
            if (stream.active()) {
                streams.put(stream.listingId(), stream.streamId());
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

    private static PropositionFilter propositions(final RuleSelector.EventFilter filter) {
        return new PropositionFilter(filter.propositionKinds(), filter.propositionScopes());
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
