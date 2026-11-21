# Reproducible validation

Prerequisites: Java 21 and Docker Engine reachable from the same user running Maven. No personal browser session or GUI is required. Run `./mvnw -B -ntp verify` (Windows `./mvnw.cmd -B -ntp verify`). Unit reports are in target/surefire-reports; real Kafka+PostgreSQL integration/failure reports are in target/failsafe-reports. The build must fail if Docker is absent; container tests are not silently skipped.

JUnit parameterized cases count individually. ContractsTest covers validation, dispatch, canonical content, v1/v2 compatibility, transitions and analytics input money. StreamingIT covers stored intake idempotency/409/size limits; keyed ordering; independent groups; duplicate revenue/count; actual DB-commit/offset gap; malformed/unknown contracts and DLT continuation; Kafka transaction rollback/retry and commit/abort visibility; schema evolution; business-order quarantine; stopped-consumer lag/catch-up; two-member assignments and rebalance; latch-controlled partition parallelism and concurrent aggregate lifecycle ordering; broker pause; real PostgreSQL constraint failure/rollback; malicious headers; DLT outage; isolated projection deletion/rebuild.

Failure hooks exist as a no-op production interface. Only the integration test supplies throwing/barrier behavior. No HTTP or environment knob enables crash injection in production. Awaitility, latches, group assignments and Admin offsets synchronize behavior. The smoke script polls HTTP with a bounded deadline; it does not sleep for a fixed presumed completion time.

The concurrency smoke uses a fixed small dataset, three worker threads and three partitions, with two consumers per projection group. It reports elapsed milliseconds for context, not a universal throughput benchmark. See evidence.md for actual run counts, durations and environment. A test assertion is evidence only after execution; source code alone is not a passed proof.

CI Verify runs Java 21 unit/integration/failure proofs, Container independently builds the image and performs disposable Compose clean-start plus smoke. Reports upload even on failure. Dependency review checks PR changes and Dependabot creates reviewable upgrade proposals; nothing automatically merges a major update. No Kafka Streams, ZooKeeper or in-memory broker dependency is needed.

## Invariant mapping

| Invariant | Proof |
| --- | --- |
| same key/partition, partition-only order | orderingKeyOffsetsAndIndependentGroups + concurrent lifecycle |
| no duplicate SQL effect | duplicateDeliveryHasOneLogicalEffect |
| crash-gap idempotency | crashAfterDatabaseCommitRedeliversWithoutDoubleRevenue |
| groups independently receive stream | processed_events contains state and analytics for each event |
| replay equivalence | replayNewGroupRebuildsEquivalentProjectionAfterDeletion |
| poison bounded | poisonAndUnsupportedContractsDoNotBlockPartition |
| backward compatibility | backwardCompatibleCreation + schemaV1AndV2AndBusinessDisorder |
| aborted records invisible | abortedTransactionInvisibleToReadCommitted |
| KRaft without ZooKeeper | apache/kafka:4.4.0 container + Compose process.roles + dependency audit |
