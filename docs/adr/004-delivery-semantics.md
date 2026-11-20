# At-least-once external boundaries

Status: accepted for 0.1.0-dev.

## Context

Neither intake DB→Kafka nor Kafka→projection DB has one atomic distributed commit.

## Decision

Use durable intake and explicit producer timeouts, synchronous record acknowledgements after DB commit, and persistent consumer idempotency. Return ACCEPTED separately from confirmed PUBLISHED.

## Consequences

Physical duplicates are expected. API 202 is storage acceptance. No generic exactly-once platform claim is permitted.

Validation: see the corresponding executed StreamingIT scenarios in [testing](../testing.md) and [evidence](../evidence.md).
