package group.gnometrading.collection.model;

/**
 * A source's capacity as placement reads it from {@code md.source}: markets per connection, and how many connections
 * the venue allows per IP, key or account ({@code connectionsPerScope} 0 for no limit). A signed source collects with
 * one of its market-data accounts, which each shard is bound to.
 */
public record SourceLimits(String code, int maxPerConnection, Scope scope, int connectionsPerScope, boolean signed) {

    /** What a venue's connection limit counts. */
    public enum Scope {
        IP,
        KEY,
        ACCOUNT
    }

    /**
     * Connections a shard opens for {@code streams} of this source: as many as the streams need, plus one spare, so a
     * make-before-break roll always has room for its new connection. None for none.
     */
    public int connectionsFor(final int streams) {
        if (streams == 0) {
            return 0;
        }
        return (streams + maxPerConnection - 1) / maxPerConnection + 1;
    }
}
