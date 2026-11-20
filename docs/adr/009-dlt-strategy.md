# Transactional poison isolation

Status: accepted for 0.1.0-dev.

## Context

Malformed JSON and unsupported contracts must not permanently block a healthy partition; DB outages must not be mistaken for poison.

## Decision

Classify deterministic input validation once and send metadata-only DLT plus source offset in one Kafka transaction. State business-order inconsistencies go to PostgreSQL quarantine with dedup. Infrastructure failures retry.

## Consequences

DLT failure intentionally retains/stalls input until restored. Original retained topic is needed to investigate payload. Redrive is an explicit future operator action, not automatic replay.

Validation: see the corresponding executed StreamingIT scenarios in [testing](../testing.md) and [evidence](../evidence.md).
