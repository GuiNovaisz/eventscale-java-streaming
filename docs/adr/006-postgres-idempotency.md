# Persistent projection deduplication

Status: accepted for 0.1.0-dev.

## Context

A consumer can crash after DB commit before the offset becomes safe.

## Decision

Insert a unique version/consumer/eventId marker and mutate the read model inside one JDBC TransactionTemplate. Serialize same-aggregate mutations with DB advisory locks. Compare content on duplicate IDs; quarantine conflicts.

## Consequences

The constraint is cross-process and survives restarts. Deleting dedup without rebuilding its projection breaks the guarantee. New event IDs for duplicate business facts remain an upstream error.

Validation: see the corresponding executed StreamingIT scenarios in [testing](../testing.md) and [evidence](../evidence.md).
