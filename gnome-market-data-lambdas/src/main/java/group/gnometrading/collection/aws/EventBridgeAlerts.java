package group.gnometrading.collection.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import group.gnometrading.collection.ports.Alerts;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;

/**
 * Alerts as {@code AlertEvent}s on the registry's {@code gnome-alerts} bus (gnome-registry {@code alerts/model.ts}),
 * whose router records them, posts them to Slack and pages on prod CRITICALs. Severity comes from the registry's type
 * catalog. A failure to publish is logged, never thrown: alerting mustn't stop a tick, and the next one fires again.
 */
public final class EventBridgeAlerts implements Alerts {

    static final String BUS = "gnome-alerts";
    private static final Logger LOGGER = LogManager.getLogger(EventBridgeAlerts.class);

    private final EventBridgeClient events;
    private final ObjectMapper mapper;
    private final Clock clock;

    public EventBridgeAlerts(final EventBridgeClient events, final ObjectMapper mapper, final Clock clock) {
        this.events = events;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Override
    public void firing(final String type, final String key, final String title, final Map<String, String> labels) {
        put(event("FIRING", type, key, title, labels));
    }

    @Override
    public void resolved(final String type, final String key) {
        put(event("RESOLVED", type, key, type + " cleared", Map.of()));
    }

    Map<String, Object> event(
            final String action,
            final String type,
            final String key,
            final String title,
            final Map<String, String> labels) {
        final Map<String, Object> event = new LinkedHashMap<>();
        event.put("action", action);
        event.put("key", key);
        event.put("type", type);
        event.put("source", "ingest");
        event.put("title", title);
        event.put("labels", labels == null ? Map.of() : labels);
        event.put("stateAt", clock.instant().toString());
        return event;
    }

    private void put(final Map<String, Object> event) {
        try {
            final String detail = mapper.writeValueAsString(event);
            events.putEvents(request -> request.entries(PutEventsRequestEntry.builder()
                    .eventBusName(BUS)
                    .source("gnome.alerts")
                    .detailType("Alert")
                    .detail(detail)
                    .build()));
        } catch (JsonProcessingException | RuntimeException error) {
            LOGGER.warn("Couldn't publish alert {}: {}", event.get("key"), error.toString());
        }
    }
}
