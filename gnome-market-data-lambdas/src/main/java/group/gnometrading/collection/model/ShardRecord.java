package group.gnometrading.collection.model;

import java.util.List;
import java.util.Map;

/**
 * A shard's item in {@code collection-shards}, which the shard itself reads as its assignment (gnome-orchestrator
 * {@code DynamoAssignment}): the streams it owns, the ones it still collects while their hand-off to another shard
 * completes ({@code draining}), the account each signed source is bound to, the hand-off cuts, and its orchestrator
 * version. Every change raises {@code generation}, written only if the stored one hasn't moved, so two ticks can't both
 * apply.
 *
 * @param cuts stream id to a minute since the epoch: for a stream in {@code streams}, the first minute it writes
 *     ({@link #WARMING}: none yet); for one in {@code draining}, the first it no longer writes. A hand-off sets both
 *     sides to the same minute, so each minute is written once
 * @param publish the streams it carries (owned or draining) that an ACTIVE session lease wants: the shard offers
 *     only these over Aeron (gnome-orchestrator {@code StreamPublisherAgent})
 *
 * @param emptySinceNanos when it last became empty, 0 while it has streams: an ACTIVE shard empty long enough is
 *     deleted
 * @param mergeableSinceNanos since when all its streams would fit on its partition's other shards, 0 while they
 *     wouldn't: one mergeable long enough drains into them ({@code ConsolidationStep})
 */
public record ShardRecord(
        String shardId,
        Partition partition,
        long generation,
        List<Integer> streams,
        List<Integer> draining,
        Map<String, String> accounts,
        Map<Integer, Integer> cuts,
        List<Integer> publish,
        String version,
        State state,
        long createdAtNanos,
        long emptySinceNanos,
        long mergeableSinceNanos) {

    /** A cut meaning: collect, but write no minute until the hand-off sets one. */
    public static final int WARMING = Integer.MAX_VALUE;

    public enum State {
        /** Takes new streams. */
        ACTIVE,
        /**
         * Being rolled, merged or scaled down: takes nothing new; its streams hand off elsewhere, then it is deleted.
         */
        DRAINING,
        /** Its service is gone; the item is kept for history until cleaned up. */
        DELETED
    }

    public ShardRecord withAssignment(
            final List<Integer> newStreams,
            final List<Integer> newDraining,
            final Map<Integer, Integer> newCuts,
            final List<Integer> newPublish) {
        return new ShardRecord(
                shardId,
                partition,
                generation,
                newStreams,
                newDraining,
                accounts,
                newCuts,
                newPublish,
                version,
                state,
                createdAtNanos,
                emptySinceNanos,
                mergeableSinceNanos);
    }

    public ShardRecord withAccounts(final Map<String, String> newAccounts) {
        return new ShardRecord(
                shardId,
                partition,
                generation,
                streams,
                draining,
                Map.copyOf(newAccounts),
                cuts,
                publish,
                version,
                state,
                createdAtNanos,
                emptySinceNanos,
                mergeableSinceNanos);
    }

    public ShardRecord withState(final State newState) {
        return new ShardRecord(
                shardId,
                partition,
                generation,
                streams,
                draining,
                accounts,
                cuts,
                publish,
                version,
                newState,
                createdAtNanos,
                emptySinceNanos,
                mergeableSinceNanos);
    }

    public ShardRecord withEmptySince(final long nanos) {
        return new ShardRecord(
                shardId,
                partition,
                generation,
                streams,
                draining,
                accounts,
                cuts,
                publish,
                version,
                state,
                createdAtNanos,
                nanos,
                mergeableSinceNanos);
    }

    public ShardRecord withMergeableSince(final long nanos) {
        return new ShardRecord(
                shardId,
                partition,
                generation,
                streams,
                draining,
                accounts,
                cuts,
                publish,
                version,
                state,
                createdAtNanos,
                emptySinceNanos,
                nanos);
    }

    public ShardRecord withGeneration(final long newGeneration) {
        return new ShardRecord(
                shardId,
                partition,
                newGeneration,
                streams,
                draining,
                accounts,
                cuts,
                publish,
                version,
                state,
                createdAtNanos,
                emptySinceNanos,
                mergeableSinceNanos);
    }

    /** A new shard: no streams yet, generation -1 until first written. */
    public static ShardRecord created(
            final String shardId, final Partition partition, final String version, final long nowNanos) {
        return new ShardRecord(
                shardId,
                partition,
                -1,
                List.of(),
                List.of(),
                Map.of(),
                Map.of(),
                List.of(),
                version,
                State.ACTIVE,
                nowNanos,
                0,
                0);
    }

    public boolean isEmpty() {
        return streams.isEmpty() && draining.isEmpty();
    }

    public boolean carries(final int streamId) {
        return streams.contains(streamId) || draining.contains(streamId);
    }
}
