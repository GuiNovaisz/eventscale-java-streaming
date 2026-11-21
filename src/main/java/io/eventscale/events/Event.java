package io.eventscale.events;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

public record Event(UUID eventId, String eventType, int eventVersion, String aggregateId,
                    Instant occurredAt, String producer, String correlationId, JsonNode payload) {}
