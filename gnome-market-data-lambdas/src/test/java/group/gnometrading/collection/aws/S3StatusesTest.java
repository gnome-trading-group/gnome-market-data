package group.gnometrading.collection.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.collection.model.StreamState;
import group.gnometrading.collection.model.TaskStatus;
import group.gnometrading.collection.model.Transition;
import java.util.List;
import org.junit.jupiter.api.Test;

class S3StatusesTest {

    @Test
    void readsTheStatusTheShardWrites() throws Exception {
        // As gnome-orchestrator's ShardStatusWriter encodes it.
        String json =
                """
                {"shard_id":"use1-1","task_id":"4f2c9e","private_ip":"10.128.0.17","version":"1.23.0",
                 "started_at_ns":5,"written_at_ns":99,"applied_generation":7,
                 "process":{"kind":"process","streams":2},
                 "connections":[{"name":"KALSHI-0","source":"KALSHI","account":"md-1","markets":2,"reconnecting":false}],
                 "streams":[{"id":12,"state":"LIVE","last_event_ns":90,"last_receive_ns":91,"last_verified_ns":80,
                             "resyncs":0,"records":3},
                            {"id":13,"state":"FAILED","last_event_ns":0,"last_receive_ns":0,"last_verified_ns":0,
                             "resyncs":0,"records":0,"failure":"IllegalStateException: bad"}],
                 "transitions_recorded":3,
                 "transitions":[[12,"NONE","SUBSCRIBING",10],[12,"SUBSCRIBING","LIVE",20],[13,"NONE","FAILED",30]]}
                """;

        TaskStatus status = S3Statuses.parse(new ObjectMapper().readTree(json));

        assertEquals(
                new TaskStatus(
                        "use1-1",
                        "4f2c9e",
                        99,
                        List.of(new StreamState(12, "LIVE"), new StreamState(13, "FAILED")),
                        List.of(
                                new Transition(12, "NONE", "SUBSCRIBING", 10),
                                new Transition(12, "SUBSCRIBING", "LIVE", 20),
                                new Transition(13, "NONE", "FAILED", 30))),
                status);
        assertEquals("collection/shards/use1-1/status/", S3Statuses.prefix("use1-1"));
    }
}
