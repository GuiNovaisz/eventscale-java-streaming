package io.eventscale;
import java.util.UUID;
public final class Samples {
    private Samples() {}
    public static String event(UUID id,String order,String type,int version) {
        return """
          {"eventId":"%s","aggregateId":"%s","eventType":"%s","eventVersion":%d,
           "occurredAt":"2026-11-01T12:00:00Z","producer":"commerce-api","correlationId":"trace-test",
           "payload":{"orderId":"%s","customerId":"customer-1","total":42.50,"currency":"BRL"%s}}
          """.formatted(id,order,type,version,order,version==2?",\"channel\":\"web\"":"");
    }
    public static String created(String order) { return event(UUID.randomUUID(),order,"order.created",1); }
}
