package io.eventscale.kafka;

/** No production controls or HTTP failure endpoints. Integration tests replace this bean. */
public interface FailureProbe {
    default void afterDatabaseCommit(String consumer, java.util.UUID eventId) {}
    default void afterKafkaWrite(java.util.UUID eventId) {}
    default void beforeProjection(String consumer, java.util.UUID eventId) {}
    default void beforeDeadLetter(String key) {}
    default void afterDeadLetterFailure(String key) {}
}
