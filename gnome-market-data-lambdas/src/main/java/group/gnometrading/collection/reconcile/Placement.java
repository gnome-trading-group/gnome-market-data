package group.gnometrading.collection.reconcile;

import group.gnometrading.collection.model.Account;
import group.gnometrading.collection.model.Partition;
import group.gnometrading.collection.model.ShardCaps;
import group.gnometrading.collection.model.SourceLimits;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Puts streams on shards (the reconciler's place step): each onto the fullest accepting shard of its partition that has
 * room, so streams pack onto few shards; failing that onto a new shard. Room means:
 *
 * <ul>
 *   <li>the shard stays within its tier's {@link ShardCaps}: streams, and its load (published streams, a leased
 *       stream counting, and summed records per second, together). An empty shard takes any one stream, so a stream
 *       busier than a whole shard is still collected rather than left unplaced;
 *   <li>its connections for the source ({@link SourceLimits#connectionsFor}) stay within the source's limit per IP,
 *       counted per shard, as each runs on its own instance;
 *   <li>for a source limited per account or key, the connections of every shard bound to that account stay within its
 *       budget. A shard is bound to an account the first time it gets a stream of the source: the active account with
 *       the most room. A stream no account has room for stays unplaced, its reason {@link Unplaced.Reason#BUDGET}.
 * </ul>
 *
 * <p>A new shard is started only while its partition has fewer than its {@code max_shards}; a stream that would need
 * one past that stays unplaced, its reason {@link Unplaced.Reason#CAPACITY}. A stream moving make-before-break off a
 * draining shard (a roll or a merge) may take the partition one shard over, as the shard it leaves goes once empty.
 *
 * <p>Pure: it reads loads and writes decisions, so the reconciler applies them and tests check them.
 */
public final class Placement {

    /**
     * A stream to place. {@code makeBeforeBreak}: it is leaving a draining shard, so a new shard for it may take the
     * partition one over its {@code max_shards}. {@code published}: a session leases it. {@code recordsPerSecond}: what
     * it adds to a shard's load. {@code avoidShardId}: the shard it is being moved off, which mustn't take it back;
     * null for none.
     */
    public record Request(
            int streamId,
            String source,
            Partition partition,
            boolean makeBeforeBreak,
            boolean published,
            double recordsPerSecond,
            String avoidShardId) {
        public Request(final int streamId, final String source, final Partition partition) {
            this(streamId, source, partition, false, false, 0, null);
        }
    }

    public record Assigned(int streamId, String shardId) {}

    public record Unplaced(int streamId, Reason reason) {
        public enum Reason {
            /** No account of its source has a connection to spare. */
            BUDGET,
            /** Its source can't be collected at all: unknown, or its limits allow no stream on a shard. */
            SOURCE,
            /** It needs a new shard, and its partition already runs its {@code max_shards}. */
            CAPACITY
        }
    }

    /** The decisions, and the shards created for them (also among {@link #loads}). */
    public record Result(List<Assigned> assigned, List<ShardLoad> created, List<Unplaced> unplaced) {}

    private final Map<String, SourceLimits> sources;
    private final Map<String, List<Account>> accounts;
    private final Function<Partition, ShardCaps> caps;
    private final Map<Partition, Integer> maxShards;
    private final Function<Partition, String> newShardId;

    /**
     * @param caps each partition's shard caps
     * @param maxShards the most shards each listed partition may run; a partition not listed has no cap
     */
    public Placement(
            final Map<String, SourceLimits> sources,
            final Map<String, List<Account>> accounts,
            final Function<Partition, ShardCaps> caps,
            final Map<Partition, Integer> maxShards,
            final Function<Partition, String> newShardId) {
        this.sources = sources;
        this.accounts = accounts;
        this.caps = caps;
        this.maxShards = maxShards;
        this.newShardId = newShardId;
    }

    /** Places {@code requests} in order (production first is the caller's), adding to {@code loads} as it goes. */
    public Result place(final List<ShardLoad> loads, final List<Request> requests) {
        final List<Assigned> assigned = new ArrayList<>();
        final List<ShardLoad> created = new ArrayList<>();
        final List<Unplaced> unplaced = new ArrayList<>();
        for (Request request : requests) {
            final SourceLimits limits = sources.get(request.source());
            if (limits == null || limits.maxPerConnection() <= 0) {
                unplaced.add(new Unplaced(request.streamId(), Unplaced.Reason.SOURCE));
                continue;
            }
            ShardLoad shard = fullestWithRoom(loads, request, limits);
            if (shard == null) {
                if (atMaxShards(loads, request)) {
                    unplaced.add(new Unplaced(request.streamId(), Unplaced.Reason.CAPACITY));
                    continue;
                }
                final ShardLoad fresh =
                        new ShardLoad(newShardId.apply(request.partition()), request.partition(), true, Map.of());
                if (!fits(loads, fresh, limits, request)) {
                    unplaced.add(new Unplaced(
                            request.streamId(), limits.signed() ? Unplaced.Reason.BUDGET : Unplaced.Reason.SOURCE));
                    continue;
                }
                loads.add(fresh);
                created.add(fresh);
                shard = fresh;
            }
            bindIfNeeded(loads, shard, limits);
            shard.add(limits.code(), request.published(), request.recordsPerSecond());
            assigned.add(new Assigned(request.streamId(), shard.shardId()));
        }
        return new Result(assigned, created, unplaced);
    }

    /** Whether another shard would take the request's partition past its cap (one past it for a make-before-break). */
    private boolean atMaxShards(final List<ShardLoad> loads, final Request request) {
        final Integer max = maxShards.get(request.partition());
        if (max == null) {
            return false;
        }
        final long running = loads.stream()
                .filter(load -> load.partition().equals(request.partition()))
                .count();
        return running >= (request.makeBeforeBreak() ? max + 1L : max);
    }

    private ShardLoad fullestWithRoom(final List<ShardLoad> loads, final Request request, final SourceLimits limits) {
        return loads.stream()
                .filter(shard -> shard.accepting()
                        && shard.partition().equals(request.partition())
                        && !Objects.equals(shard.shardId(), request.avoidShardId()))
                .sorted(Comparator.comparingInt((ShardLoad shard) -> -shard.streams())
                        .thenComparing(ShardLoad::shardId))
                .filter(shard -> fits(loads, shard, limits, request))
                .findFirst()
                .orElse(null);
    }

    /** Whether the request's stream fits on {@code shard}, binding an account if it would need one. */
    private boolean fits(
            final List<ShardLoad> loads, final ShardLoad shard, final SourceLimits limits, final Request request) {
        if (!withinCaps(shard, request)) {
            return false;
        }
        final int before = limits.connectionsFor(shard.streams(limits.code()));
        final int after = limits.connectionsFor(shard.streams(limits.code()) + 1);
        if (limits.scope() == SourceLimits.Scope.IP
                && limits.connectionsPerScope() > 0
                && after > limits.connectionsPerScope()) {
            return false;
        }
        if (!limits.signed()) {
            return true;
        }
        final String bound = shard.accounts().get(limits.code());
        if (bound != null) {
            final Account account = account(limits.code(), bound);
            return account != null && account.active() && room(loads, limits, account) >= after - before;
        }
        return bestAccount(loads, limits, after) != null;
    }

    private boolean withinCaps(final ShardLoad shard, final Request request) {
        final ShardCaps shardCaps = caps.apply(shard.partition());
        // An empty shard always takes its first stream, however busy, or that stream could never be placed.
        return shard.streams() + 1 <= shardCaps.maxStreams()
                && (shard.streams() == 0
                        || shardCaps.load(
                                        shard.published() + (request.published() ? 1 : 0),
                                        shard.recordsPerSecond() + request.recordsPerSecond())
                                <= 1);
    }

    private void bindIfNeeded(final List<ShardLoad> loads, final ShardLoad shard, final SourceLimits limits) {
        if (!limits.signed() || shard.accounts().containsKey(limits.code())) {
            return;
        }
        final int needed = limits.connectionsFor(1);
        shard.bind(limits.code(), bestAccount(loads, limits, needed).label());
    }

    /** The active account with the most room, if it has at least {@code needed}; null if none has. */
    private Account bestAccount(final List<ShardLoad> loads, final SourceLimits limits, final int needed) {
        Account best = null;
        int bestRoom = Integer.MIN_VALUE;
        for (Account account : accounts.getOrDefault(limits.code(), List.of())) {
            if (!account.active()) {
                continue;
            }
            final int room = room(loads, limits, account);
            if (room >= needed && room > bestRoom) {
                best = account;
                bestRoom = room;
            }
        }
        return best;
    }

    /** Connections the account has left over every shard bound to it; unlimited is {@link Integer#MAX_VALUE}. */
    private static int room(final List<ShardLoad> loads, final SourceLimits limits, final Account account) {
        final int budget = account.connectionBudget() > 0 ? account.connectionBudget() : limits.connectionsPerScope();
        if (budget == 0 || limits.scope() == SourceLimits.Scope.IP) {
            return Integer.MAX_VALUE;
        }
        int used = 0;
        for (ShardLoad shard : loads) {
            if (account.label().equals(shard.accounts().get(limits.code()))) {
                used += limits.connectionsFor(shard.streams(limits.code()));
            }
        }
        return budget - used;
    }

    private Account account(final String source, final String label) {
        for (Account account : accounts.getOrDefault(source, List.of())) {
            if (account.label().equals(label)) {
                return account;
            }
        }
        return null;
    }
}
