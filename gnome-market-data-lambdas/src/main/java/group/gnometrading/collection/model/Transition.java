package group.gnometrading.collection.model;

public record Transition(int streamId, String from, String to, long atNanos) {}
