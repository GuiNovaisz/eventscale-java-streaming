# Explicit versioned event contracts

```json
{
  "eventId": "dd0bb5e6-1b93-4e7c-b109-25bb75e198d4",
  "eventType": "order.created",
  "eventVersion": 2,
  "aggregateId": "order-123",
  "occurredAt": "2026-11-01T12:00:00Z",
  "producer": "commerce-api",
  "correlationId": "checkout-123",
  "payload": {"orderId":"order-123","customerId":"customer-9","total":42.50,"currency":"BRL","channel":"web"}
}
```

eventId is a global UUID used for dedup/audit/tracing, never a Kafka offset. aggregateId is the Kafka key and must match the string payload.orderId. occurredAt is immutable event time within years 0001 through 9999 (representable by the downstream storage contract); intake.ingested_at is admission time, processing logs/durations capture processing time. producer names provenance; correlationId joins a request flow and is propagated as a byte header and in the envelope, never a partition key or metric label. Payload identities and optional channel must be strings, without scalar coercion.

| Type | Versions | Required payload |
| --- | --- | --- |
| order.created | 1, 2 | orderId, customerId, total, currency |
| payment.confirmed | 1 | orderId, total, currency |
| order.cancelled | 1 | orderId |
| order.shipped | 1 | orderId |
| order.delivered | 1 | orderId |

Optional `channel` in creation v2 defaults to `unknown` when absent (including old v1). Total must be a nonnegative JSON number with at most two decimal places and at most 999999999999.99. Currency must match three uppercase letters. IDs/provenance/correlation/channel have bounded lengths. API body is at most 65536 UTF-8 bytes. Unsupported versions/types fail HTTP validation and go to DLT if received directly in the raw Kafka topic. No arbitrary Java type headers are interpreted.

The normalized topic preserves event identity, version, event time and correlation; adds channel default only for creation. Source raw JSON and normalized JSON therefore need not be byte-identical. Dedup content comparison occurs within each boundary. Unknown optional payload fields are tolerated but remain part of canonical intake identity. This is explicit version dispatch and tested backward compatibility, not universal forward compatibility or a schema registry.

See [OpenAPI](openapi.yaml) for HTTP paths, status and payload schema. Queries return asynchronous projections and may temporarily return 404 before catch-up.
