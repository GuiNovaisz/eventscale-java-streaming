# Kafka for a retained event log

Status: accepted for 0.1.0-dev.

## Context

The same externally produced facts feed independent state and analytics readers and must remain replayable. A work queue would solve a different ownership/delivery problem.

## Decision

Use Kafka with retained partitioned lifecycle topics and independent group offsets. NotifyFlow remains the RabbitMQ reliable-delivery example.

## Consequences

Kafka operational cost, retention planning and partitions are real tradeoffs. RabbitMQ is not inferior; it fits work distribution rather than this retained-log reconstruction requirement.

Validation: see the corresponding executed StreamingIT scenarios in [testing](../testing.md) and [evidence](../evidence.md).
