package group.gnometrading.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.networking.http.HTTPProtocol;
import group.gnometrading.networking.http.HTTPResponse;
import group.gnometrading.networking.http.RetryableHTTPClient;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The market-data API (gnome-market-data {@code market-data-api}), called with its API key. */
public final class MarketDataApi {

    private static final String API_KEY_HEADER = "x-api-key";
    private static final String MISSING_ENDPOINT = "/api/collection/missing?";
    // The route's limits: ids per request and the longest range.
    static final int IDS_PER_REQUEST = 100;
    static final long MAX_RANGE_NANOS = 31L * 86_400 * 1_000_000_000;
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final String host;
    private final String apiKey;
    private final RetryableHTTPClient httpClient;

    public MarketDataApi(final String host, final String apiKey) {
        this(host, apiKey, new RetryableHTTPClient());
    }

    /** Not thread-safe: one per thread. */
    public MarketDataApi(final String host, final String apiKey, final RetryableHTTPClient httpClient) {
        this.host = host;
        this.apiKey = apiKey;
        this.httpClient = httpClient;
    }

    /**
     * Every stretch of [fromNs, toNs) each stream had no collected data, up to now, in as many requests as the route's
     * limits need. Streams the registry doesn't know are left out.
     */
    public List<MissingData> missing(final int[] streamIds, final long fromNs, final long toNs) {
        final Map<Integer, Accumulator> byStream = new LinkedHashMap<>();
        for (int from = 0; from < streamIds.length; from += IDS_PER_REQUEST) {
            final int to = Math.min(streamIds.length, from + IDS_PER_REQUEST);
            for (long windowFrom = fromNs; windowFrom < toNs; windowFrom += MAX_RANGE_NANOS) {
                final long windowTo = Math.min(toNs, windowFrom + MAX_RANGE_NANOS);
                final Response response = fetch(streamIds, from, to, windowFrom, windowTo);
                for (final StreamResponse stream : response.streams()) {
                    byStream.computeIfAbsent(
                                    stream.streamId(),
                                    id -> new Accumulator(stream.listingId() == null ? 0 : stream.listingId()))
                            .add(stream);
                }
            }
        }
        final List<MissingData> result = new ArrayList<>(byStream.size());
        byStream.forEach((id, acc) ->
                result.add(new MissingData(id, acc.listingId, acc.stretches, acc.missingNs, acc.untrackedNs)));
        return result;
    }

    private Response fetch(final int[] streamIds, final int from, final int to, final long fromNs, final long toNs) {
        final StringBuilder path = new StringBuilder(MISSING_ENDPOINT).append("streamIds=");
        for (int i = from; i < to; i++) {
            path.append(i == from ? "" : ",").append(streamIds[i]);
        }
        path.append("&fromNs=").append(fromNs).append("&toNs=").append(toNs);
        try {
            final HTTPResponse response =
                    this.httpClient.get(HTTPProtocol.HTTPS, this.host, path.toString(), API_KEY_HEADER, this.apiKey);
            if (!response.isSuccess()) {
                throw new IllegalStateException(
                        "Unable to request the market-data API. Status code: " + response.getStatusCode());
            }
            return OBJECT_MAPPER.readValue(toByteArray(response.getBody()), Response.class);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to request the market-data API", e);
        }
    }

    private static byte[] toByteArray(final ByteBuffer buffer) {
        final ByteBuffer copy = buffer.duplicate();
        final byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    /** One stream's stretches across requests; a stretch cut by a window boundary is joined again. */
    private static final class Accumulator {
        private final int listingId;
        private final List<MissingStretch> stretches = new ArrayList<>();
        private long missingNs;
        private long untrackedNs;

        Accumulator(final int listingId) {
            this.listingId = listingId;
        }

        void add(final StreamResponse stream) {
            for (final MissingStretch stretch : stream.missing()) {
                final int last = this.stretches.size() - 1;
                if (last >= 0
                        && this.stretches.get(last).toNs() == stretch.fromNs()
                        && this.stretches.get(last).reason().equals(stretch.reason())) {
                    this.stretches.set(
                            last,
                            new MissingStretch(this.stretches.get(last).fromNs(), stretch.toNs(), stretch.reason()));
                } else {
                    this.stretches.add(stretch);
                }
            }
            this.missingNs += stream.missingNs();
            this.untrackedNs += stream.untrackedNs();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Response(List<StreamResponse> streams) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record StreamResponse(
            int streamId, Integer listingId, List<MissingStretch> missing, long missingNs, long untrackedNs) {}
}
