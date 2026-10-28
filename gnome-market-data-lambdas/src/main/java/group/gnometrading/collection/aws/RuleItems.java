package group.gnometrading.collection.aws;

import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.RuleSelector;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * {@code collection-rules} items as rules. The rules API writes them; {@code parameters} holds the type's fields in
 * snake_case, as its schemas name them, except an {@code event} rule's ids: a top-level number set {@code event_ids},
 * because DynamoDB adds to and deletes from a set atomically only at the top level.
 */
final class RuleItems {

    static final String STATIC_STREAMS = "static_streams";
    static final String EVENT = "event";
    static final String EVENT_FILTER = "event_filter";

    private RuleItems() {}

    /** The rule an item holds; null for a type this reconciler doesn't know. */
    static Rule parse(final Map<String, AttributeValue> item) {
        final Map<String, AttributeValue> parameters =
                item.containsKey("parameters") && item.get("parameters").hasM()
                        ? item.get("parameters").m()
                        : Map.of();
        final RuleSelector selector =
                switch (string(item.get("rule_type"))) {
                    case STATIC_STREAMS -> new RuleSelector.StaticStreams(ints(parameters.get("stream_ids")));
                    case EVENT -> new RuleSelector.EventIds(numberSet(item.get("event_ids")));
                    case EVENT_FILTER -> new RuleSelector.EventFilter(
                            List.copyOf(ints(parameters.get("exchange_ids"))),
                            strings(parameters.get("categories")),
                            strings(parameters.get("tags")),
                            integer(parameters.get("expires_within_hours")),
                            decimal(parameters.get("min_volume_24h")),
                            integer(parameters.get("top_n")),
                            integer(parameters.get("max_streams")));
                    default -> null;
                };
        if (selector == null) {
            return null;
        }
        final Set<Integer> streams =
                selector instanceof RuleSelector.StaticStreams statics ? statics.streamIds() : Set.of();
        return new Rule(
                string(item.get("rule_id")),
                selector,
                streams,
                Priority.valueOf(string(item.get("priority"))),
                string(item.get("tier")),
                integer(item.get("retire_grace_minutes")));
    }

    private static String string(final AttributeValue value) {
        return value == null ? "" : value.s();
    }

    private static Integer integer(final AttributeValue value) {
        return value == null || value.n() == null ? null : Integer.parseInt(value.n());
    }

    private static Double decimal(final AttributeValue value) {
        return value == null || value.n() == null ? null : Double.parseDouble(value.n());
    }

    private static Set<Integer> ints(final AttributeValue value) {
        final Set<Integer> ints = new HashSet<>();
        if (value != null && value.hasL()) {
            value.l().forEach(v -> ints.add(Integer.parseInt(v.n())));
        }
        return ints;
    }

    /** A top-level number set: an {@code event} rule's ids live there so they can be added and removed atomically. */
    private static Set<Integer> numberSet(final AttributeValue value) {
        final Set<Integer> ints = new HashSet<>();
        if (value != null && value.hasNs()) {
            value.ns().forEach(v -> ints.add(Integer.parseInt(v)));
        }
        return ints;
    }

    private static List<String> strings(final AttributeValue value) {
        return value != null && value.hasL()
                ? value.l().stream().map(AttributeValue::s).toList()
                : List.of();
    }
}
