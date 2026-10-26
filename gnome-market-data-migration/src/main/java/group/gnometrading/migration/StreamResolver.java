package group.gnometrading.migration;

import group.gnometrading.SecurityMaster;
import group.gnometrading.md.Stream;
import group.gnometrading.sm.Listing;
import java.util.HashMap;
import java.util.Map;

/** The book stream of the listing a legacy record named by exchange and security, from the registry. */
final class StreamResolver {

    private final SecurityMaster securityMaster;
    private final Map<Long, Integer> streams = new HashMap<>();

    StreamResolver(final SecurityMaster securityMaster) {
        this.securityMaster = securityMaster;
    }

    /**
     * @throws IllegalStateException if no listing has that exchange and security, or the listing has no book stream
     */
    synchronized int streamId(final int exchangeId, final long securityId) {
        final long key = ((long) exchangeId << Integer.SIZE) | securityId;
        final Integer cached = this.streams.get(key);
        if (cached != null) {
            return cached;
        }
        final Listing listing = this.securityMaster.getListing(exchangeId, Math.toIntExact(securityId));
        if (listing == null) {
            throw new IllegalStateException(
                    "No listing for exchange " + exchangeId + " security " + securityId + " in the registry");
        }
        final Stream stream = this.securityMaster.streamForListing(listing.listingId());
        if (stream == null) {
            throw new IllegalStateException("Listing " + listing.listingId() + " has no book stream");
        }
        this.streams.put(key, stream.streamId());
        return stream.streamId();
    }
}
