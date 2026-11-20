# Safe projection rebuild

Replay reads retained committed events again to rebuild a derived model. Retry repeats a failed attempt. Redrive moves selected quarantined/dead-letter work back into processing after an operator decision. Reprocessing applies potentially new logic to historical input; replay is one mechanism. These terms are not synonyms.

Never delete active projection dedup markers or reset an active group's offsets as the routine rebuild method. Seven-day retention is a hard bound: earliest means **earliest still retained**, not the beginning of business history. Before rebuilding, inspect beginning offsets and confirm the required history remains; otherwise restore from archive/snapshot, which this milestone does not implement.

## Rebuild with the supplied admin script

1. Start Compose, preserve intake/Kafka volumes and active v1 tables.
2. Choose a never-used version (e.g. rebuild_20261120) and ensure both derived topic and its contract history remain retained.
3. `pwsh ./scripts/replay.ps1 -Version rebuild_20261120` starts another instance with normalizer/dispatcher disabled and no HTTP server. It creates versioned rows and two new groups at earliest. It does not reset production offsets or activate the model.
4. Inspect `order-state-projector-rebuild_20261120` and `analytics-projector-rebuild_20261120` with the group command in operations.md. The process remains a continuous consumer; its groups continue receiving live events.
5. At a quiescent validation boundary (pause upstream admissions operationally), wait for both active and new groups to catch up. Compare ordered state columns, analytics counts/revenue by currency, and quarantine categories. Exclude version/processing timestamps, retain business event IDs/time. `StreamingIT` contains executable deterministic snapshots.
6. Explicitly activate only after validation: `docker compose exec -T postgres psql -U eventscale -d eventscale -v ON_ERROR_STOP=1 -c "UPDATE active_projection SET version='rebuild_20261120' WHERE singleton=true;"`. The version FK prevents nonexistent targets; the operator must perform the lag/equivalence checks first. API reads switch by one DB row update.
7. Keep the new instance running to maintain the active model. Old consumers can remain for rollback until an explicit later cleanup. Rollback switches active_projection back to a still-current version. Do not remove the only process maintaining the active version.

The script creates a task-owned container named `eventscale-replay-VERSION`; stop/remove it explicitly after its model is no longer active. Compose's standard `down` does not own one-off replay containers. Reusing a version/group resumes offsets; it is **not** a fresh replay. Use a new version for another rebuild.

## Reproducible proof

The integration suite publishes a known dataset and waits for both projections, snapshots original v1, builds a candidate and compares it to v1, deletes only candidate order_state/analytics/processed_events/quarantine rows, starts a new group at earliest, then asserts exact ordered state and analytics equality. The original active projection and Kafka/intake records remain intact. Random UUIDs vary per run; equivalence compares those same identities within the run. No retention expiry is simulated as a successful full replay.
