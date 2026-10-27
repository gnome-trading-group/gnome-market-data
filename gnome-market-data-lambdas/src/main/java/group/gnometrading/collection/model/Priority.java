package group.gnometrading.collection.model;

/** Production streams (session leases) are placed before research ones (rules) and may evict them; never the reverse. */
public enum Priority {
    PRODUCTION,
    RESEARCH
}
