# Kafka-native transactional normalization

Status: accepted for 0.1.0-dev.

## Context

Normalization supplies a compatible default and needs atomic input-offset/output visibility without involving PostgreSQL.

## Decision

Use Spring Kafka transactional listener container plus transactional template and read_committed downstream. Validate poison into a DLT within the same transaction. Evaluate but omit Streams: no state stores, windows or joins justify another API.

## Consequences

EOS is scoped to Kafka read-process-write. Callback/metrics attempts can repeat. Instance transaction prefixes must be unique; DLT failure aborts rather than dropping input.

Validation: see the corresponding executed StreamingIT scenarios in [testing](../testing.md) and [evidence](../evidence.md).
