package group.gnometrading.collection.model;

/**
 * One of a source's market-data accounts ({@code md.source_account}): {@code connectionBudget} overrides the source's
 * {@code connections_per_scope} for it (0 keeps the source's). Only active ones are bound to new shards.
 */
public record Account(String source, String label, int connectionBudget, boolean active) {}
