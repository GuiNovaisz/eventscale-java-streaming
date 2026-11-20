# New version and new groups

Status: accepted for 0.1.0-dev.

## Context

Rebuilding an active read model destructively or resetting its live offsets creates operational risk.

## Decision

Consume the retained normalized topic with new versioned state/analytics groups at earliest, compare snapshots at a quiescent boundary, then explicitly switch the active pointer. Keep the new consumers running.

## Consequences

No active model deletion or offset reset is required. Seven-day retention bounds reconstruction; archives and snapshot bootstrapping remain future work.

Validation: see the corresponding executed StreamingIT scenarios in [testing](../testing.md) and [evidence](../evidence.md).
