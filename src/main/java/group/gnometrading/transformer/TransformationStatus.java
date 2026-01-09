package group.gnometrading.transformer;

public enum TransformationStatus {
    PENDING,
    BLOCKED,
    PROCESSING,
    COMPLETE,
    FAILED,
    GAP_UNEXPECTED,
    GAP_EXPECTED
}

