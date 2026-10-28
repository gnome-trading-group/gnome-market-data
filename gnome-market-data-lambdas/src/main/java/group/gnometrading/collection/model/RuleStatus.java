package group.gnometrading.collection.model;

/**
 * What a rule last resolved to, written on its item for the UI: how many events and streams it selects, how many of an
 * {@code event} rule's events have no active stream yet ({@code waitingEvents}), and whether {@code max_streams} left
 * some out.
 */
public record RuleStatus(int events, int streams, int waitingEvents, boolean capped, long resolvedAtNanos) {

    /** The same selection, whenever it was resolved. */
    public boolean sameSelection(final RuleStatus other) {
        return other != null
                && events == other.events
                && streams == other.streams
                && waitingEvents == other.waitingEvents
                && capped == other.capped;
    }
}
