package io.eventscale.events;

import java.math.BigDecimal;
import java.util.Set;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** Explicit dispatch; incoming headers never select a Java class. */
@Component
public class Contracts {
    public static final int MAX_BYTES = 65536;
    private static final Set<String> TYPES = Set.of("order.created", "payment.confirmed", "order.cancelled", "order.shipped", "order.delivered");
    private final JsonMapper mapper = JsonMapper.builder().build();

    public Event decode(String json) {
        try {
            if (json == null || json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_BYTES)
                throw new InvalidEvent("SIZE_LIMIT");
            Event event = mapper.readValue(json, Event.class);
            validate(event);
            return event;
        } catch (InvalidEvent e) { throw e; }
        catch (Exception e) { throw new InvalidEvent("MALFORMED_JSON"); }
    }

    public void validate(Event e) {
        if (e == null || e.eventId() == null || e.occurredAt() == null || !text(e.aggregateId(), 128)
                || !text(e.producer(), 100) || !text(e.correlationId(), 128)) throw new InvalidEvent("INVALID_ENVELOPE");
        if (e.eventType() == null || !TYPES.contains(e.eventType())) throw new InvalidEvent("UNKNOWN_TYPE");
        if (e.eventVersion() != 1 && !(e.eventType().equals("order.created") && e.eventVersion() == 2))
            throw new InvalidEvent("UNSUPPORTED_VERSION");
        JsonNode p = e.payload();
        if (p == null || !p.isObject()) throw new InvalidEvent("INVALID_PAYLOAD");
        if (!e.aggregateId().equals(p.path("orderId").asText())) throw new InvalidEvent("KEY_MISMATCH");
        if (e.eventType().equals("order.created") || e.eventType().equals("payment.confirmed")) {
            if (!p.path("total").isNumber() || p.path("total").decimalValue().signum() < 0
                    || p.path("total").decimalValue().scale() > 2
                    || p.path("total").decimalValue().compareTo(new BigDecimal("999999999999.99")) > 0
                    || !p.path("currency").asText().matches("[A-Z]{3}")) throw new InvalidEvent("INVALID_MONEY");
        }
        if (e.eventType().equals("order.created") && !text(p.path("customerId").asText(), 128))
            throw new InvalidEvent("INVALID_CUSTOMER");
        if (p.has("channel") && !text(p.path("channel").asText(), 64)) throw new InvalidEvent("INVALID_CHANNEL");
    }

    public String canonical(Event event) { return canonicalNode(mapper.valueToTree(event)).toString(); }
    private JsonNode canonicalNode(JsonNode node) {
        if (node.isObject()) {
            ObjectNode result = mapper.createObjectNode();
            java.util.TreeSet<String> names = new java.util.TreeSet<>(node.propertyNames());
            names.forEach(name -> result.set(name, canonicalNode(node.get(name))));
            return result;
        }
        if (node.isArray()) {
            var result = mapper.createArrayNode();
            node.forEach(item -> result.add(canonicalNode(item)));
            return result;
        }
        return node;
    }
    public String normalize(Event e) {
        ObjectNode payload = (ObjectNode) e.payload().deepCopy();
        if (e.eventType().equals("order.created") && !payload.has("channel")) payload.put("channel", "unknown");
        return canonical(new Event(e.eventId(), e.eventType(), e.eventVersion(), e.aggregateId(), e.occurredAt(), e.producer(), e.correlationId(), payload));
    }
    private boolean text(String value, int max) { return value != null && !value.isBlank() && value.length() <= max; }
    public static class InvalidEvent extends RuntimeException {
        public InvalidEvent(String category) { super(category); }
    }
}
