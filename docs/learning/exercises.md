# Exercises — no solutions supplied

1. Publish four lifecycle events for one order and collect topic/partition/offset evidence. State precisely which ordering was demonstrated.
2. Publish three orders concurrently and compare per-order sequences with the global interleaving. Do not compare unrelated partition offsets as a global clock.
3. Stop analytics consumption while state continues. Record positive lag, then catch-up. Explain why backlog alone is not an error.
4. Submit the same API event twice, then change its payload under the same eventId. Capture responses and inspect the unique storage row.
5. Trigger the test-only post-DB-commit failure. Explain each physical delivery and every logical revenue mutation.
6. Introduce an invalid version and malformed JSON before a valid record with the same key. Locate the DLT coordinates and subsequent successful projection.
7. Make DLT unavailable in an isolated test. Inspect source group offsets before restoring it. Explain the availability/data-loss tradeoff.
8. Create a committed and aborted Kafka transaction and compare read_committed with a separate read_uncommitted diagnostic consumer.
9. Introduce a backward-compatible optional v3 payload field as an experimental branch. Define fixtures and unsupported-version behavior before changing code.
10. Rebuild into a new projection version, compare deterministic snapshots and describe activation/rollback. Never reset a live group's offsets for the exercise.
11. Send shipment before payment and explain the difference between Kafka order and business order, including analytics consequences.
12. Design, without deploying it, a partition-expansion migration that preserves the stated per-aggregate contract.
13. Estimate when seven-day retention becomes insufficient and propose an archive/snapshot protocol with explicit correctness assumptions.
14. Inspect the metric tags and identify how an eventId label would change cardinality. Keep identifiers in logs only.

Run destructive experiments only against task-owned disposable containers/volumes, never another application's environment.
