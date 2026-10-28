package group.gnometrading.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import group.gnometrading.networking.http.HTTPProtocol;
import group.gnometrading.networking.http.HTTPResponse;
import group.gnometrading.networking.http.RetryableHTTPClient;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class MarketDataApiTest {

    private static final long DAY = 86_400L * 1_000_000_000;
    private static final long WINDOW = MarketDataApi.MAX_RANGE_NANOS;

    private final RetryableHTTPClient client = mock(RetryableHTTPClient.class);
    private final MarketDataApi api = new MarketDataApi("md.test", "key", client);

    private void respond(final String path, final String body) throws IOException {
        final HTTPResponse response = mock(HTTPResponse.class);
        when(response.isSuccess()).thenReturn(true);
        when(response.getBody()).thenReturn(ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)));
        when(client.get(eq(HTTPProtocol.HTTPS), eq("md.test"), eq(path), eq("x-api-key"), eq("key")))
                .thenReturn(response);
    }

    @Test
    void readsEachStreamsStretches() throws IOException {
        respond(
                "/api/collection/missing?streamIds=7,9&fromNs=0&toNs=100",
                """
                {"fromNs":0,"toNs":100,"trackedSinceNs":10,"unknownIds":[],"streams":[
                  {"streamId":7,"listingId":70,"missing":[{"fromNs":40,"toNs":60,"reason":"TASK_LOST"}],
                   "missingNs":20,"untrackedNs":10},
                  {"streamId":9,"listingId":90,"missing":[],"missingNs":0,"untrackedNs":10}]}
                """);

        assertEquals(
                List.of(
                        new MissingData(7, 70, List.of(new MissingStretch(40, 60, "TASK_LOST")), 20, 10),
                        new MissingData(9, 90, List.of(), 0, 10)),
                api.missing(new int[] {7, 9}, 0, 100));
    }

    @Test
    void aLongRangeIsAskedForInWindowsAndAStretchAcrossABoundaryIsJoined() throws IOException {
        respond(
                "/api/collection/missing?streamIds=7&fromNs=0&toNs=" + WINDOW,
                "{\"streams\":[{\"streamId\":7,\"missing\":[{\"fromNs\":" + (WINDOW - DAY) + ",\"toNs\":" + WINDOW
                        + ",\"reason\":\"NOT_COLLECTED\"}],\"missingNs\":" + DAY + ",\"untrackedNs\":0}]}");
        respond(
                "/api/collection/missing?streamIds=7&fromNs=" + WINDOW + "&toNs=" + (WINDOW + DAY),
                "{\"streams\":[{\"streamId\":7,\"missing\":[{\"fromNs\":" + WINDOW + ",\"toNs\":" + (WINDOW + 5)
                        + ",\"reason\":\"NOT_COLLECTED\"}],\"missingNs\":5,\"untrackedNs\":0}]}");

        assertEquals(
                List.of(new MissingData(
                        7, 0, List.of(new MissingStretch(WINDOW - DAY, WINDOW + 5, "NOT_COLLECTED")), DAY + 5, 0)),
                api.missing(new int[] {7}, 0, WINDOW + DAY));
    }

    @Test
    void aFailedRequestFailsLoudly() throws IOException {
        final HTTPResponse forbidden = mock(HTTPResponse.class);
        when(forbidden.getStatusCode()).thenReturn(403);
        when(client.get(any(HTTPProtocol.class), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(forbidden);

        assertThrows(IllegalStateException.class, () -> api.missing(new int[] {7}, 0, 100));
    }
}
