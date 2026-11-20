# Aggregate identity as key

Status: accepted for 0.1.0-dev.

## Context

Lifecycle events require order per external order, while unrelated orders should progress concurrently.

## Decision

Use aggregateId=payload.orderId as Kafka record key, never correlationId. Keep three partitions fixed in this milestone and serialize intake admission per aggregate.

## Consequences

Ordering is partition-local, not global or event-time sorting. Upstream writers still own semantic order; partition expansion requires migration.

Validation: see the corresponding executed StreamingIT scenarios in [testing](../testing.md) and [evidence](../evidence.md).
