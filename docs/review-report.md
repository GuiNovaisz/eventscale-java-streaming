# v0.1.0 — Event Streaming Foundation review report

Historical snapshot: this report records the foundation review at feature SHA `67a886a0e0cae34eccfaef2be1ac211768b4c0a3`, before the subsequently authorized integration into dev. Its branch/review instructions and CI findings describe that review stage. Current integration state is recorded by [PR #1](https://github.com/GuiNovaisz/eventscale-java-streaming/pull/1); release preparation remains pending and the project version stays `0.1.0-dev`.

## Executive Summary

Implemented EventScale as a retained commerce event log with independent state/analytics consumers, non-destructive replay and precise transaction boundaries. Development version remains 0.1.0-dev. Local Maven and clean-start Compose validations passed. The latest remote checks are linked below and must be green before this mission is marked ready.

## Versions

Java 21 LTS (local Temurin 21.0.13.1+2), Spring Boot 4.2.0, Spring Kafka 4.2.0, kafka-clients/broker 4.4.0, PostgreSQL 18.7, Testcontainers 2.0.5, Maven 3.9.12. Official source/advisory decisions: [versions](versions.md).

## Git

Repository: [GuiNovaisz/eventscale-java-streaming](https://github.com/GuiNovaisz/eventscale-java-streaming), public, MIT, default main. Bootstrap main/dev share `abebeb6377acdc975c826946a816c4cd7a264f3a`. Audited implementation commit: `91cceacf1f9a953740a475064348b449d62f6ddb`; subsequent documentation commits remain on the same feature. [PR #1](https://github.com/GuiNovaisz/eventscale-java-streaming/pull/1) targets dev from feature/event-streaming-foundation and is attached to [milestone #1](https://github.com/GuiNovaisz/eventscale-java-streaming/milestone/1). main/dev require PR approval, strict verify/container/review checks, resolved conversations, and reject force-push/deletion (including admins). No merge, tag or Release was performed.

## Architecture

External facts → durable JDBC intake → ordered dispatcher → raw Kafka → transactional normalizer → normalized Kafka → two independent JDBC projectors. A consolidated Spring deployable avoids artificial service boundaries. [Architecture](architecture.md).

## Kafka Topology

| Topic | Partitions | Replication / minISR | Retention / cleanup | Producer | Consumers |
| --- | ---: | --- | --- | --- | --- |
| eventscale.orders.v1 | 3 | 1 / 1 | 7 days / delete | intake dispatcher | eventscale-normalizer-v1 |
| eventscale.order-events-normalized.v1 | 3 | 1 / 1 | 7 days / delete | transactional normalizer | order-state-projector-VERSION, analytics-projector-VERSION |
| eventscale.orders.dlt.v1 | 3 | 1 / 1 | 7 days / delete | transactional normalizer | administrative read_committed inspection |

Code-declared topology, KRaft only, no auto-create. Max message/decompressed body 131072 bytes, LogAppendTime for retention. RF1 is not HA.

## Event Contracts

UUID identity, explicit type/version, aggregate key, event time/provenance/correlation and bounded JSON payload. Five lifecycle facts; creation v1/v2. [Contract](event-contracts.md), [OpenAPI](openapi.yaml).

## Partitioning

aggregateId=orderId key, fixed three partitions. Same key maps consistently; different keys can share a partition or run concurrently. Partition expansion requires a migration plan.

## Ordering

Per-partition log order and serialized intake admission per aggregate, with no global or semantic business-order promise. Single-order offsets [0,1,2,3] and concurrent aggregate lifecycle/parallel consumer barriers were proved. Out-of-order business transitions enter quarantine.

## Consumer Groups

Independent state/analytics groups each use two consumers across three partitions. Tests inspect disjoint assignments and survivor reassignment on member loss, and assert both groups' persisted effects.

## Delivery Semantics

Kafka idempotent producer handles protocol retry duplicates; durable business identity remains eventId. Intake publication and JDBC projections tolerate at-least-once delivery. Projection marker/mutation commit atomically in PostgreSQL. Crash after DB commit before listener return redelivers without duplicate revenue.

## Transactions

Kafka-only normalization commits output/DLT and input offset atomically; read_committed hides aborted transactions. A post-output injected failure yields two attempts and one visible output. PostgreSQL is outside that Kafka transaction; no distributed exactly-once claim is made. [Delivery](delivery-semantics.md).

## Projections

State derives lifecycle; analytics independently counts valid facts and sums payment revenue by currency, without windows or accounting semantics. Persistent uniqueness and aggregate locks prevent duplicate/corrupt effects under the defined model.

## Replay

New versions/groups, earliest retained normalized history, deterministic comparison, explicit pointer activation. Test destroys only candidate projections/dedup and rebuilds equivalent data (44 orders, six analytics rows). Operational script separately rebuilt the Compose smoke dataset without changing the active model. [Replay](replay.md).

## Schema Evolution

Current consumer handles creation v1 and v2, adding optional channel/default unknown. Unsupported versions are quarantined at raw normalization. No universal forward compatibility or registry governance claim.

## DLT

Malformed/type/version failures go to bounded transactional metadata-only DLT. Real destination deletion proves the source offset cannot advance past poison; restoration resumes progress. DB failures retry; business-order inconsistencies use SQL quarantine. [Failure model](failure-model.md).

## Failure Proofs

F1 duplicate, F2 post-DB-commit crash, F3 poison continuation, F4 real broker pause, F5 stopped consumer backlog/catch-up, F6 rebalance, F7 replay, F8 aborted transaction. Additional DB constraint rollback, malicious headers, DLT outage and concurrent lifecycles run against real dependencies. [Evidence](evidence.md).

## Observability

Actuator dependency readiness/liveness and Prometheus production, failures, consumption, duplicates, quarantine, DLT, duration and per-group lag. Lag observed zero for all three main groups after catch-up. Fixed consumer/category tags; identifiers only in structured logs. [Operations](operations.md).

## Tests

20 unit + 16 integration (including failure proofs) = 36 tests; failures=0, errors=0, skips=0. There is no separate additive failure suite. Final local integration duration 49.31 seconds, including a real DLT publication timeout/recovery; unit duration 0.559 seconds. Maven also includes container lifecycle/shutdown. [Test mapping](testing.md).

## Docker

Non-root Java image; single Kafka KRaft broker/controller, PostgreSQL and app. Clean-start and HTTP smoke passed on a separate project/port, preserving user containers. RF1/local plaintext only. Compose environment variables supply secrets.

## CI

Verify: Java 21, Maven unit + real Kafka/PostgreSQL integration/failure proofs with uploaded reports. Container: Docker build, clean-start, smoke, isolated cleanup. [Initial PR Verify run](https://github.com/GuiNovaisz/eventscale-java-streaming/actions/runs/44832292988) and [dependency review](https://github.com/GuiNovaisz/eventscale-java-streaming/actions/runs/44832293122) passed. The final revision is gated on the [latest PR checks](https://github.com/GuiNovaisz/eventscale-java-streaming/pull/1/checks), including both push and PR builds. Dependabot configuration is delivered on the feature and activates when it reaches the default branch in a later authorized merge; vulnerability alerts are enabled now. No automatic major merges are configured.

## Security

Official Kafka/Spring advisories reviewed, patched selected versions, decompression bound, raw StringDeserializer and SimpleKafkaHeaderMapper, explicit JSON decode, body bounds and parameterized JDBC. Health detail restricted; only health/Prometheus exposed. Repository audit found no actual secrets, .env, keystores, dumps or user-local absolute paths. Local API/plaintext Kafka are loopback-only; public deployment requires separate security design.

## Documentation

Complete PT-BR/English README, Mermaid architecture, event/delivery/partition/replay/schema/failure/testing/operations/version documents, nine ADRs, OpenAPI and learning glossary/interview guide/exercises without solutions.

## Findings

Critical: 0 open after audit. Important: 0 open after local verification; latest remote CI remains a mandatory completion gate. Optional: multi-broker HA, archived replay beyond retention, schema registry, intake/dedup lifecycle, stronger public deployment security and targeted larger benchmarks. These are declared scope limits, not implemented claims. Task-owned Compose/replay containers and disposable volumes were removed after validation; other running applications were preserved.
