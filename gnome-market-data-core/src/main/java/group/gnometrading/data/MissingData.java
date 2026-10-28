package group.gnometrading.data;

import java.util.List;

/**
 * Where one stream had no collected data over a range; {@code listingId} is the listing it carries the book of, or 0
 * for a vendor stream. {@code untrackedNs} is time before collection coverage was
 * recorded: whether data was collected then is unknown, so it is never a stretch.
 */
public record MissingData(
        int streamId, int listingId, List<MissingStretch> missing, long missingNs, long untrackedNs) {}
