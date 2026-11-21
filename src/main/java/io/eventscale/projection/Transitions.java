package io.eventscale.projection;

public final class Transitions {
    private Transitions() {}
    public static String apply(String current, String eventType) {
        return switch (eventType) {
            case "order.created" -> require(current == null, "CREATED");
            case "payment.confirmed" -> require("CREATED".equals(current), "PAID");
            case "order.cancelled" -> require("CREATED".equals(current) || "PAID".equals(current), "CANCELLED");
            case "order.shipped" -> require("PAID".equals(current), "SHIPPED");
            case "order.delivered" -> require("SHIPPED".equals(current), "DELIVERED");
            default -> throw new IllegalArgumentException("Unknown event type");
        };
    }
    private static String require(boolean valid, String next) { if (!valid) throw new InconsistentEvent(); return next; }
    public static class InconsistentEvent extends RuntimeException {}
}
