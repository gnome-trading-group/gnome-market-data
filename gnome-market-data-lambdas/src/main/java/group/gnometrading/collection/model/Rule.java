package group.gnometrading.collection.model;

import java.util.Set;

/** A rule as evaluation reads it; only {@code static_streams} so far (Phase 7 adds the rest). */
public record Rule(String ruleId, Set<Integer> streamIds, Priority priority, String tier, Integer retireGraceMinutes) {}
