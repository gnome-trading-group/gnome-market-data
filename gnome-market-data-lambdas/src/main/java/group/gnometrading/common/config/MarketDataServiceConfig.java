package group.gnometrading.common.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The {@code market-data} entry of the controller's Service Config ({@code GET config/market-data}), read as the
 * classifier reads its own: {@link MarketDataConfig#DEFAULTS} go along in {@code x-config-defaults}, so the first read
 * creates the entry and later reads add any new keys, people's edits winning. What comes back is merged over the
 * defaults, so a key still missing keeps its default. Refreshed every {@link #REFRESH_NANOS}; a failed read keeps the
 * last good config, or the defaults before any.
 */
public final class MarketDataServiceConfig implements Supplier<MarketDataConfig> {

    public static final long REFRESH_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final Logger LOGGER = LogManager.getLogger(MarketDataServiceConfig.class);

    private final HttpClient http;
    private final ObjectMapper mapper;
    private final String url;
    private final Supplier<String> apiKey;
    private final LongSupplier nanoClock;
    private final String defaultsHeader;
    private MarketDataConfig current;
    private long fetchedAtNanos;

    /** @param controllerApiUrl the controller API's base URL, ending in a slash */
    public MarketDataServiceConfig(
            final HttpClient http,
            final ObjectMapper mapper,
            final String controllerApiUrl,
            final Supplier<String> apiKey,
            final LongSupplier nanoClock) {
        this.http = http;
        this.mapper = mapper;
        this.url = controllerApiUrl + "config/market-data";
        this.apiKey = apiKey;
        this.nanoClock = nanoClock;
        try {
            this.defaultsHeader = Base64.getEncoder()
                    .encodeToString(
                            mapper.writeValueAsString(MarketDataConfig.DEFAULTS).getBytes(StandardCharsets.UTF_8));
        } catch (Exception impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    @Override
    public synchronized MarketDataConfig get() {
        final long now = nanoClock.getAsLong();
        if (current != null && now - fetchedAtNanos < REFRESH_NANOS) {
            return current;
        }
        try {
            current = parse(fetch());
            fetchedAtNanos = now;
        } catch (Exception error) {
            LOGGER.warn("Couldn't read the market-data service config; keeping what we had: {}", error.toString());
            if (current == null) {
                current = MarketDataConfig.DEFAULTS;
            }
        }
        return current;
    }

    /** {@code stored} over the defaults, so every field has a value. */
    public MarketDataConfig parse(final JsonNode stored) throws Exception {
        final ObjectNode merged = mapper.valueToTree(MarketDataConfig.DEFAULTS);
        merge(merged, stored);
        return mapper.treeToValue(merged, MarketDataConfig.class);
    }

    private static void merge(final ObjectNode into, final JsonNode from) {
        if (from == null || !from.isObject()) {
            return;
        }
        final Iterator<Map.Entry<String, JsonNode>> fields = from.fields();
        while (fields.hasNext()) {
            final Map.Entry<String, JsonNode> field = fields.next();
            final JsonNode existing = into.get(field.getKey());
            if (existing instanceof ObjectNode nested && field.getValue().isObject()) {
                merge(nested, field.getValue());
            } else {
                into.set(field.getKey(), field.getValue());
            }
        }
    }

    private JsonNode fetch() throws Exception {
        final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create(url))
                        .timeout(Duration.ofSeconds(5))
                        .header("x-api-key", apiKey.get())
                        .header("x-config-defaults", defaultsHeader)
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("HTTP " + response.statusCode() + ": " + response.body());
        }
        return mapper.readTree(response.body()).path("config");
    }
}
