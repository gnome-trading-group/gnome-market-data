package group.gnometrading.collection.model;

import java.util.List;
import java.util.Set;

/** What a rule collects ({@code rule_type} and {@code parameters} of its {@code collection-rules} item). */
public sealed interface RuleSelector {

    /** {@code static_streams}: these streams. */
    record StaticStreams(Set<Integer> streamIds) implements RuleSelector {}

    /**
     * {@code event}: every stream of these events, both sides of every contract, including contracts added later. Other
     * systems add and remove ids atomically through the rules API; the reconciler removes an event once it resolves.
     */
    record EventIds(Set<Integer> eventIds) implements RuleSelector {}

    /**
     * {@code event_filter}: every stream of the open events matching every given filter (empty lists and nulls match
     * all), at most {@code maxStreams} streams. With {@code propositionKinds} or {@code propositionScopes} only the
     * contracts whose proposition matches count: an event matches through them, and only their streams (and the
     * event's vendor streams) are taken. {@code startsWithinHours} keeps events starting within that many hours, or
     * already started; events with no start time never match it. With {@code minVolume24h} or {@code topN} events are ranked by 24-hour
     * volume, and an event joins only at or above {@code minVolume24h} and within the top {@code topN}; one already
     * collected stays while it keeps half that volume and ranks within 1.5 times {@code topN}. Without them events
     * rank by soonest expiry.
     */
    record EventFilter(
            List<Integer> exchangeIds,
            List<String> categories,
            List<String> tags,
            List<String> propositionKinds,
            List<String> propositionScopes,
            Integer expiresWithinHours,
            Integer startsWithinHours,
            Double minVolume24h,
            Integer topN,
            int maxStreams)
            implements RuleSelector {

        public boolean byVolume() {
            return minVolume24h != null || topN != null;
        }
    }

    /**
     * {@code listing_filter}: the book stream of every active listing on any of {@code exchangeIds} whose security is any
     * of {@code securityTypes} (sm.security.type names, e.g. {@code PERPETUAL}; empty lists match all), largest 24-hour
     * volume first, at most {@code maxStreams} streams. With {@code minVolume24h} or {@code topN}, a listing joins only
     * at or above {@code minVolume24h} and within the top {@code topN}; one already collected stays while it keeps half
     * that volume and ranks within 1.5 times {@code topN}.
     */
    record ListingFilter(
            List<Integer> exchangeIds, List<String> securityTypes, Double minVolume24h, Integer topN, int maxStreams)
            implements RuleSelector {}
}
