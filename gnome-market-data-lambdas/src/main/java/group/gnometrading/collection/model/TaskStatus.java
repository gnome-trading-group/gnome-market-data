package group.gnometrading.collection.model;

import java.util.List;

/** What a task's status says, as the reconciler parsed it. */
public record TaskStatus(
        String shardId, String taskId, long writtenAtNanos, List<StreamState> streams, List<Transition> transitions) {}
