package group.gnometrading.collection.model;

import java.util.Set;

/**
 * A stream the control plane collects, or is retiring ({@code collection-targets}): which rules want it, its partition,
 * the shard it is on (null until placed), and since when it has gone uncovered ({@code uncoveredSinceNanos} 0 while
 * covered), with why: the gap written once coverage resumes.
 *
 * <p>While it hands off to {@code shardId}, {@code drainingFromShardId} still collects and writes it, and
 * {@code shardId} collects it without writing. Once {@code shardId} has covered it for the hand-off time (from
 * {@code handoffCoveredSinceNanos}), the hand-off picks {@code cutMinute}: the old shard writes the minutes before it,
 * the new one from it on, and after it the old shard lets the stream go.
 *
 * @param retireGraceMinutes the wanting rules' grace, kept for when none wants it any more; null for the default
 */
public record Target(
        int streamId,
        String source,
        Partition partition,
        Set<String> wantedBy,
        Integer retireGraceMinutes,
        State state,
        String shardId,
        long retireAfterNanos,
        long uncoveredSinceNanos,
        String uncoveredReason,
        String drainingFromShardId,
        long handoffCoveredSinceNanos,
        int cutMinute) {

    public enum State {
        DESIRED,
        RETIRING,
        RETIRED
    }

    /** Not placed yet: uncovered from creation, as {@code NOT_PLACED}. */
    public static final String NOT_PLACED = "NOT_PLACED";

    public static Target desired(
            final int streamId,
            final String source,
            final Partition partition,
            final Set<String> wantedBy,
            final Integer retireGraceMinutes,
            final long nowNanos) {
        return new Target(
                streamId,
                source,
                partition,
                Set.copyOf(wantedBy),
                retireGraceMinutes,
                State.DESIRED,
                null,
                0,
                nowNanos,
                NOT_PLACED,
                null,
                0,
                0);
    }

    public Target desired(final Partition newPartition, final Set<String> rules, final Integer graceMinutes) {
        return copy().partition(newPartition)
                .wantedBy(Set.copyOf(rules))
                .grace(graceMinutes)
                .state(State.DESIRED)
                .retireAfter(0)
                .build();
    }

    public Target retiring(final long afterNanos) {
        return copy().wantedBy(Set.of())
                .state(State.RETIRING)
                .retireAfter(afterNanos)
                .build();
    }

    public Target retired() {
        return copy().wantedBy(Set.of()).state(State.RETIRED).build();
    }

    public Target onShard(final String shard) {
        return copy().shard(shard).build();
    }

    /** Moves to {@code shard}, {@code from} still collecting it until the hand-off completes. */
    public Target handingOff(final String shard, final String from) {
        return copy().shard(shard)
                .drainingFrom(from)
                .handoffCoveredSince(0)
                .cut(0)
                .build();
    }

    public Target handoffCoveredSince(final long nanos) {
        return copy().handoffCoveredSince(nanos).build();
    }

    /** The hand-off cuts at {@code minute}; 0 takes the cut back. */
    public Target cutAt(final int minute) {
        return copy().cut(minute).build();
    }

    public Target handedOff() {
        return copy().drainingFrom(null).handoffCoveredSince(0).cut(0).build();
    }

    /** Coverage stopped at {@code atNanos} for {@code reason}, or (reason null) resumed. */
    public Target uncovered(final long atNanos, final String reason) {
        return copy().uncoveredSince(reason == null ? 0 : atNanos)
                .uncoveredReason(reason)
                .build();
    }

    private Builder copy() {
        return new Builder(this);
    }

    private static final class Builder {
        private final int streamId;
        private final String source;
        private Partition partition;
        private Set<String> wantedBy;
        private Integer retireGraceMinutes;
        private State state;
        private String shardId;
        private long retireAfterNanos;
        private long uncoveredSinceNanos;
        private String uncoveredReason;
        private String drainingFromShardId;
        private long handoffCoveredSinceNanos;
        private int cutMinute;

        Builder(final Target target) {
            this.streamId = target.streamId;
            this.source = target.source;
            this.partition = target.partition;
            this.wantedBy = target.wantedBy;
            this.retireGraceMinutes = target.retireGraceMinutes;
            this.state = target.state;
            this.shardId = target.shardId;
            this.retireAfterNanos = target.retireAfterNanos;
            this.uncoveredSinceNanos = target.uncoveredSinceNanos;
            this.uncoveredReason = target.uncoveredReason;
            this.drainingFromShardId = target.drainingFromShardId;
            this.handoffCoveredSinceNanos = target.handoffCoveredSinceNanos;
            this.cutMinute = target.cutMinute;
        }

        Builder partition(final Partition value) {
            partition = value;
            return this;
        }

        Builder wantedBy(final Set<String> value) {
            wantedBy = value;
            return this;
        }

        Builder grace(final Integer value) {
            retireGraceMinutes = value;
            return this;
        }

        Builder state(final State value) {
            state = value;
            return this;
        }

        Builder shard(final String value) {
            shardId = value;
            return this;
        }

        Builder retireAfter(final long value) {
            retireAfterNanos = value;
            return this;
        }

        Builder uncoveredSince(final long value) {
            uncoveredSinceNanos = value;
            return this;
        }

        Builder uncoveredReason(final String value) {
            uncoveredReason = value;
            return this;
        }

        Builder drainingFrom(final String value) {
            drainingFromShardId = value;
            return this;
        }

        Builder cut(final int value) {
            cutMinute = value;
            return this;
        }

        Builder handoffCoveredSince(final long value) {
            handoffCoveredSinceNanos = value;
            return this;
        }

        Target build() {
            return new Target(
                    streamId,
                    source,
                    partition,
                    wantedBy,
                    retireGraceMinutes,
                    state,
                    shardId,
                    retireAfterNanos,
                    uncoveredSinceNanos,
                    uncoveredReason,
                    drainingFromShardId,
                    handoffCoveredSinceNanos,
                    cutMinute);
        }
    }
}
