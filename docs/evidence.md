# Executed evidence — 2026-11-20

## Local final Maven verification

Command: Maven Wrapper-equivalent Maven 3.9.12, `-B -ntp verify`, Java 21.0.13.1+2. Final audited test reports were produced on 2026-11-20 America/Sao_Paulo. Maven includes container lifecycle/shutdown in addition to the suite durations below.

| Actual report suite | Tests | Failures | Errors | Skips | Reported suite duration |
| --- | ---: | ---: | ---: | ---: | --- |
| ContractsTest / Surefire (unit) | 20 | 0 | 0 | 0 | 0.559 s |
| StreamingIT / Failsafe (integration including failure proofs) | 16 | 0 | 0 | 0 | 49.31 s |
| Total | 36 | 0 | 0 | 0 | — |

F1–F8 are scenarios inside StreamingIT, not eight extra test cases. The DLT-unavailable scenario is also included. Kafka image apache/kafka:4.4.0 runs KRaft, PostgreSQL image postgres:18.7-alpine is real, and Docker absence is not skipped. No fake or embedded broker is used.

Selected machine-readable proof lines from that successful execution:

```text
ORDERING_PROOF key=ordered partition=0 offsets=[0, 1, 2, 3] groups=state,analytics
CRASH_PROOF attempts=2 revenue=42.50 count=1
EOS_STAGE_PROOF attempts=2 read_committed_outputs=1
GROUP_PROOF assignments=[[eventscale.order-events-normalized.v1-0, eventscale.order-events-normalized.v1-1], [eventscale.order-events-normalized.v1-2]]
CONTROLLED_SMOKE events=73 partitions=3 stateConsumers=2 durationMs=2744
REPLAY_PROOF orders=44 analyticsRows=6 equal=true newConsumerGroup=true
```

The 73-event controlled concurrency smoke uses 18 concurrent aggregate lifecycles (four facts each) plus one latch-blocked order. Context: Windows amd64, AMD Ryzen 7 5700X, 8 cores/16 logical processors, approximately 32 GiB host RAM; Docker Desktop Linux Engine 29.9.2 with 15957 MB available to Docker; three producer workers; two consumers per projection group. This small approximately 2.7-second scenario is a correctness/throughput smoke, not a universal events/sec benchmark. It includes a deterministic blocked consumer and is not a stress test.

## Compose clean-start and operational replay

An isolated project `eventscale-foundation-review` was run through `docker compose down -v` and `docker compose up -d --build --wait --wait-timeout 180`. Only this task's volumes were disposable. Port 18082 was selected because another user service already owned 8080. Existing applications were preserved. All three services became healthy; Flyway migrated the new database and application topics were created with auto-create disabled.

The HTTP smoke published four lifecycle events, returned PASS/DELIVERED, confirmed one partition (1) and raw offsets [0,1,2,3]. Analytics contained one fact per type and payment revenue BRL 42.50. The supplied replay script started `review_replay` with two new groups, normalizer/dispatcher disabled and no HTTP server. It rebuilt one order and four analytics rows equivalent to active v1 without changing the active pointer.

Observed Prometheus samples after catch-up:

```text
eventscale_consumer_lag{group="analytics-projector-v1"} 0.0
eventscale_consumer_lag{group="eventscale-normalizer-v1"} 0.0
eventscale_consumer_lag{group="order-state-projector-v1"} 0.0
```

## Corrections validated during development

- Used Boot's Kafka starter so modular autoconfiguration supplies KafkaAdmin.
- Configured LogAppendTime retention independently from preserved occurredAt, so historical event timestamps do not prematurely age newly normalized history.
- Enabled fixTxOffsets for read_committed projectors so transactional control records do not leave misleading caught-up lag.
- Disabled broker auto-create in Testcontainers as well as Compose; otherwise deleting DLT would silently recreate it and invalidate its outage proof.
- Managed both producer factories as lifecycle beans.
- Bounded event time to the storage-supported contract and rejected non-string payload identity/channel scalars.
- Strengthened DLT outage synchronization to await a real publication failure before restoring the topic, then proved retry/catch-up.

See review-report.md for remote CI links and final repository state. Source tests and archived CI report artifacts provide reproducibility; no raw local machine paths or credentials are embedded in this evidence.
