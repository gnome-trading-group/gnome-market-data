package group.gnometrading.collection.ports;

import java.util.Map;

/** Alerts through {@code gnome-alerts}; firing an already open key and resolving a closed one are no-ops. */
public interface Alerts {
    void firing(String type, String key, String title, Map<String, String> labels);

    void resolved(String type, String key);
}
