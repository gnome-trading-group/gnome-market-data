package group.gnometrading.collection.aws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.collection.model.StreamState;
import group.gnometrading.collection.model.TaskStatus;
import group.gnometrading.collection.model.Transition;
import group.gnometrading.collection.ports.Statuses;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Each task's latest status, as gnome-orchestrator's {@code ShardStatusWriter} wrote it to
 * {@code s3://<bucket>/collection/shards/<shard>/status/<task>.json}. A status older than {@link #MAX_AGE} is skipped:
 * its task's intervals have long been closed as lost, and the bucket's lifecycle rule removes it.
 */
public final class S3Statuses implements Statuses {

    static final Duration MAX_AGE = Duration.ofDays(1);
    private static final Logger LOGGER = LogManager.getLogger(S3Statuses.class);

    private final S3Client s3;
    private final String bucket;
    private final ObjectMapper mapper;
    private final Clock clock;

    public S3Statuses(final S3Client s3, final String bucket, final ObjectMapper mapper, final Clock clock) {
        this.s3 = s3;
        this.bucket = bucket;
        this.mapper = mapper;
        this.clock = clock;
    }

    static String prefix(final String shardId) {
        return "collection/shards/" + shardId + "/status/";
    }

    @Override
    public List<TaskStatus> latest(final String shardId) {
        final Instant oldest = clock.instant().minus(MAX_AGE);
        final List<TaskStatus> statuses = new ArrayList<>();
        for (S3Object object : s3.listObjectsV2Paginator(
                        request -> request.bucket(bucket).prefix(prefix(shardId)))
                .contents()) {
            if (object.lastModified().isBefore(oldest)) {
                continue;
            }
            try {
                final byte[] body = s3.getObjectAsBytes(
                                request -> request.bucket(bucket).key(object.key()))
                        .asByteArray();
                statuses.add(parse(mapper.readTree(body)));
            } catch (IOException | RuntimeException unreadable) {
                // One task's unreadable status mustn't stop the tick; that task reads as silent until it writes again.
                LOGGER.warn("Skipping unreadable status {}: {}", object.key(), unreadable.toString());
            }
        }
        return statuses;
    }

    static TaskStatus parse(final JsonNode status) {
        final List<StreamState> streams = new ArrayList<>();
        for (JsonNode stream : status.path("streams")) {
            streams.add(new StreamState(
                    stream.path("id").asInt(), stream.path("state").asText()));
        }
        final List<Transition> transitions = new ArrayList<>();
        for (JsonNode transition : status.path("transitions")) {
            transitions.add(new Transition(
                    transition.get(0).asInt(),
                    transition.get(1).asText(),
                    transition.get(2).asText(),
                    transition.get(3).asLong()));
        }
        return new TaskStatus(
                status.path("shard_id").asText(),
                status.path("task_id").asText(),
                status.path("written_at_ns").asLong(),
                streams,
                transitions);
    }
}
