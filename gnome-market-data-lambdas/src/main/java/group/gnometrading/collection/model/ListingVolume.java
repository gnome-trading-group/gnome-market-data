package group.gnometrading.collection.model;

/** An active listing as a rule ranks it: its 24-hour volume, 0 before the exchange sync's first reading. */
public record ListingVolume(int listingId, double volume24h) {}
