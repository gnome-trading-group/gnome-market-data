package group.gnometrading.collection.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.RegistryConnection;
import group.gnometrading.SecurityMaster;
import group.gnometrading.strings.GnomeString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RegistryAdapterTest {

    private final RegistryConnection connection = mock(RegistryConnection.class);
    private final List<String> paths = new ArrayList<>();
    private final RegistryAdapter adapter =
            new RegistryAdapter(mock(SecurityMaster.class), connection, new ObjectMapper());

    private void answer(final String body) {
        when(connection.get(any(GnomeString.class))).thenAnswer(call -> {
            paths.add(call.getArgument(0).toString());
            return ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8));
        });
    }

    @Test
    void openSessionStatusesIsOneRequestForEverySessionNotYetEnded() {
        answer("[{\"session_id\": \"s-1\", \"status\": \"RUNNING\", \"mode\": \"paper\"},"
                + " {\"session_id\": \"s-2\", \"status\": \"STARTING\"}, {\"session_id\": \"s-3\"}]");

        Map<String, String> statuses = adapter.openSessionStatuses("paper");

        assertEquals(Map.of("s-1", "RUNNING", "s-2", "STARTING"), statuses);
        assertEquals(List.of("/api/strategy-sessions?status=SUBMITTED,STARTING,RUNNING,STOPPING&mode=paper"), paths);
    }

    @Test
    void withoutAModeItAsksForEveryMode() {
        answer("[]");

        adapter.openSessionStatuses(null);

        assertEquals(List.of("/api/strategy-sessions?status=SUBMITTED,STARTING,RUNNING,STOPPING"), paths);
    }

    @Test
    void anUnreadableAnswerThrowsRatherThanReadingAsNoSessions() {
        answer("{\"message\": \"Internal server error\"}");
        assertThrows(IllegalStateException.class, () -> adapter.openSessionStatuses("paper"));

        when(connection.get(any(GnomeString.class))).thenThrow(new RuntimeException("Status code: 502"));
        assertThrows(RuntimeException.class, () -> adapter.openSessionStatuses("paper"));
    }
}
