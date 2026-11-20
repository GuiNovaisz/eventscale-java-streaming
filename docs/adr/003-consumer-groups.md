# Independent read models

Status: accepted for 0.1.0-dev.

## Context

State projection and analytics need independent progress and failure boundaries over the same facts.

## Decision

Use distinct versioned groups, each with two members for three partitions; normalizer has its own stable group.

## Consequences

One member owns a partition at a time within a group. More members than partitions add no parallelism, and rebalance can redeliver work.

Validation: see the corresponding executed StreamingIT scenarios in [testing](../testing.md) and [evidence](../evidence.md).
