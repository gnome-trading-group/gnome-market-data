package group.gnometrading.collection.ports;

import java.util.Map;

/** Alerts through {@code gnome-alerts}; firing an already open key and resolving a closed one are no-ops. */
public interface Alerts {
    /** @param sessionId the strategy session the alert is about, so the registry links it; null if none */
    void firing(String type, String key, String title, Map<String, String> labels, String sessionId);

    void resolved(String type, String key);
}
