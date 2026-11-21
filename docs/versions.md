# Version decision — 2026-11-20

Application versions below remain unchanged for release 0.1.0; only project version metadata and the explicitly authorized GitHub Actions hygiene were promoted. Historical audit documents retain their original development-version snapshots.

| Component | Selected | Source / compatibility |
| --- | --- | --- |
| Java | 21 LTS, local Temurin 21.0.13.1+2 | Java release target 21; matching java/javac, isolated tool directory |
| Spring Boot | 4.2.0 GA | [Project](https://spring.io/projects/spring-boot/), [managed coordinates](https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html) |
| Spring Kafka | 4.2.0 | Boot BOM; [project](https://spring.io/projects/spring-kafka/) |
| kafka-clients / broker | 4.4.0 / 4.4.0 | Boot BOM / [Apache downloads](https://kafka.apache.org/downloads/) |
| PostgreSQL | 18.7 | [Release notes](https://www.postgresql.org/docs/release/18.7/), stable patched 18 branch, no need for a new major |
| Testcontainers | 2.0.5 | Boot BOM, [Kafka module](https://java.testcontainers.org/modules/kafka/) using org.testcontainers.kafka.KafkaContainer |
| Maven | 3.9.12 | Official Maven Wrapper 3.3.4, only-script distribution |

Direct project pages and the current Boot BOM take precedence over stale search snippets/compatibility tables. No RC/milestone override is used. Boot's modular Kafka starter is required for Kafka autoconfiguration. PostgreSQL uses JDBC and Flyway, without ORM.

## Advisory review

Reviewed the [official Kafka CVE list](https://kafka.apache.org/community/cve-list/). Kafka 4.4.0 includes fixes for the October 2026 client/broker allocation, configuration disclosure, authentication and Streams issues. Crucially CVE-2026-114349 also requires a **bounded `max.decompressed.message.bytes`**; both broker and application topics use 131072 bytes. This is configured even though the producer does not enable compression.

Spring Kafka 4.2.0 is outside the affected ranges listed in these official advisories:

| Advisory | Relevant decision |
| --- | --- |
| [CVE-2026-41726](https://spring.io/security/cve-2026-41726/) | No DelegatingDeserializer or selector cache |
| [CVE-2026-41731](https://spring.io/security/cve-2026-41731/) | No typed JSON header mapper |
| [CVE-2026-59278](https://spring.io/security/cve-2026-59278/) | No java.net header deserialization / DNS resolution |
| [CVE-2026-59399](https://spring.io/security/cve-2026-59399/) | StringDeserializer, explicit Event decode; no header-selected class |
| [CVE-2026-59401](https://spring.io/security/cve-2026-59401/) | SimpleKafkaHeaderMapper keeps bytes; headers cannot load classes |
| [CVE-2026-59432](https://spring.io/security/cve-2026-59432/) | No arbitrary header arrays or object creation |

The hostile-header integration test exercises both selector and class metadata. Event data never enables Jackson default typing. Unknown event types and versions are rejected, not coerced into v1.

Java execution includes `-XX:-UseBASE64Intrinsics` as defense in depth while refreshing JDK images. Runtime image tags receive vendor patch updates; rebuild regularly and evaluate vendor advisories. This is not an assertion that every transitive dependency or image is vulnerability-free. Dependabot and dependency review provide ongoing checks; major upgrades require manual review.

## Technical references consulted before implementation

- [Producer settings and idempotence](https://kafka.apache.org/44/configuration/producer-configs/)
- [Consumer groups, offsets and isolation](https://kafka.apache.org/44/configuration/consumer-configs/)
- [Spring Kafka transactions](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html)
- [Spring Kafka EOS](https://docs.spring.io/spring-kafka/reference/kafka/exactly-once.html)
- [Kafka Streams concepts and processing guarantees](https://kafka.apache.org/44/streams/core-concepts/)
- [KRaft upgrade constraints](https://kafka.apache.org/41/getting-started/upgrade/)
- [Explicit serialization / header mapping](https://docs.spring.io/spring-kafka/reference/kafka/serdes.html)

Streams was evaluated: this stateless normalization and two JDBC projections gain no state-store/window/join capability from its DSL. A transactional Spring Kafka container makes the offset/output boundary more visible. Therefore no kafka-streams dependency or repository topic is included. JSON v1/v2 contracts demonstrate backward compatibility without a registry; there is no claim of full schema governance.

## Release CI action decision — 2026-11-20

Official current releases/README examples were checked before changing workflows: [checkout v7.0.1](https://github.com/actions/checkout/releases/tag/v7.0.1), [setup-java v6.3.0](https://github.com/actions/setup-java/releases/tag/v6.3.0), and [upload-artifact v7.0.2](https://github.com/actions/upload-artifact/releases/tag/v7.0.2). Workflows select stable majors v7/v6/v7. setup-java's official README marks v1–v4 deprecated. upload-artifact's current examples use v7; its action contract retains ZIP archiving, named artifacts and multiple report paths. Inspection of the separate dependency review job also exposed its v4 Node 20 warning; [dependency-review-action v5.0.1](https://github.com/actions/dependency-review-action/releases/tag/v5.0.1) and its official README confirm the Node 24 migration. That action now uses stable v5 with fail-on-severity=high unchanged. Temurin Java 21, Maven cache, if: always(), and test-evidence with Surefire/Failsafe reports are preserved. R1 registers a run-scoped random database password with add-mask before GITHUB_ENV export. No application dependency upgrade accompanies these CI changes.
