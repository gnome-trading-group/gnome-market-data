package group.gnometrading.collection.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import group.gnometrading.collection.model.Priority;
import group.gnometrading.collection.model.Rule;
import group.gnometrading.collection.model.RuleSelector;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

class RuleItemsTest {

    private static Map<String, AttributeValue> item(final String type, final Map<String, AttributeValue> parameters) {
        return Map.of(
                "rule_id", AttributeValue.fromS("r1"),
                "rule_type", AttributeValue.fromS(type),
                "priority", AttributeValue.fromS("RESEARCH"),
                "tier", AttributeValue.fromS("standard"),
                "retire_grace_minutes", AttributeValue.fromN("15"),
                "parameters", AttributeValue.fromM(parameters));
    }

    private static AttributeValue numbers(final int... values) {
        return AttributeValue.fromL(java.util.Arrays.stream(values)
                .mapToObj(v -> AttributeValue.fromN(Integer.toString(v)))
                .toList());
    }

    @Test
    void aStaticRuleNamesItsStreams() {
        final Rule rule = RuleItems.parse(item("static_streams", Map.of("stream_ids", numbers(3, 4))));
        assertEquals(new RuleSelector.StaticStreams(Set.of(3, 4)), rule.selector());
        assertEquals(Set.of(3, 4), rule.streamIds());
        assertEquals(Priority.RESEARCH, rule.priority());
        assertEquals(15, rule.retireGraceMinutes());
    }

    @Test
    void anEventRuleNamesItsEventsAndSelectsNoStreamsUntilResolved() {
        final Map<String, AttributeValue> item = new java.util.HashMap<>(item("event", Map.of()));
        item.put("event_ids", AttributeValue.fromNs(List.of("7")));
        final Rule rule = RuleItems.parse(item);
        assertEquals(new RuleSelector.EventIds(Set.of(7)), rule.selector());
        assertEquals(Set.of(), rule.streamIds());
    }

    @Test
    void anEventFilterReadsEveryParameterAndLeavesOutTheUnset() {
        final Rule rule = RuleItems.parse(item(
                "event_filter",
                Map.of(
                        "exchange_ids", numbers(1),
                        "categories", AttributeValue.fromL(List.of(AttributeValue.fromS("Sports"))),
                        "min_volume_24h", AttributeValue.fromN("2500.5"),
                        "max_streams", AttributeValue.fromN("200"))));
        assertEquals(
                new RuleSelector.EventFilter(List.of(1), List.of("Sports"), List.of(), null, 2500.5, null, 200),
                rule.selector());
    }

    @Test
    void anEventRuleWithNoIdsLeftSelectsNothing() {
        assertEquals(
                new RuleSelector.EventIds(Set.of()),
                RuleItems.parse(item("event", Map.of())).selector());
    }

    @Test
    void anUnknownTypeIsNotARule() {
        assertNull(RuleItems.parse(item("vendor_schedule", Map.of())));
    }
}
