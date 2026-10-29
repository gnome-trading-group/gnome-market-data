package group.gnometrading.collection.ports;

import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.EventVolume;
import group.gnometrading.collection.model.RuleSelector;
import group.gnometrading.collection.model.SourceLimits;
import group.gnometrading.collection.model.StreamInfo;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** What the registry says about streams, events, sources, market-data accounts and strategy sessions. */
public interface Registry {
    Map<Integer, StreamInfo> streams(Collection<Integer> streamIds);

    Map<String, SourceLimits> sources();

    Map<String, List<Account>> accounts();

    /**
     * Up to {@code limit} open events matching {@code filter}'s exchanges, categories and tags, expiring before
     * {@code expiresBeforeMillis} (0: any) with at least {@code minVolume24h} (0: any): largest volume first when
     * {@code byVolume}, else soonest expiry.
     */
    List<EventVolume> events(
            RuleSelector.EventFilter filter,
            boolean byVolume,
            double minVolume24h,
            long expiresBeforeMillis,
            int limit);

    /** Of these events, the ones resolved. */
    Set<Integer> resolvedEvents(Collection<Integer> eventIds);

    /**
     * The status of every strategy session not yet ended ({@code SUBMITTED}, {@code STARTING}, {@code RUNNING} or
     * {@code STOPPING}), by session id; only those in {@code mode} unless it is null. Throws if the registry can't be
     * read.
     */
    Map<String, String> openSessionStatuses(String mode);

    /** Each event's active streams, both sides of every contract; an event with none is left out. */
    Map<Integer, Set<Integer>> streamsOfEvents(Collection<Integer> eventIds);
}
