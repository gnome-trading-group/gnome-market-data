package group.gnometrading.collection.ports;

import group.gnometrading.collection.model.TaskStatus;
import java.util.List;

/** The latest status each of a shard's tasks wrote (gnome-orchestrator {@code ShardStatusWriter}). */
public interface Statuses {
    List<TaskStatus> latest(String shardId);
}
