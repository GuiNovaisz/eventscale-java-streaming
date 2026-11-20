# Operations runbook

All commands assume the repository root and DB_PASSWORD already set in the process environment. CLI examples use Docker Compose, with no personal browser/cookies. Never expose plaintext local services as a hosted deployment.

## Lag / outage / catch-up

`eventscale_consumer_lag{group=...}` samples admin end offsets minus committed group offsets every 10 seconds. -1 means unavailable/unknown. Lag is an offset distance, not duration or business error count. Transaction markers and aborted records can inflate naive offset distance; projectors enable Spring's fixTxOffsets with read_committed/max.poll.records=20. A stopped group accumulates backlog while the producer/normalizer continue. A returning group should converge toward zero if capacity exceeds arrival rate. Increasing lag with normal processing can indicate insufficient capacity; increasing lag plus errors can indicate DB/DLT/broker failure. No alert threshold is universally correct.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group analytics-projector-v1
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group order-state-projector-v1 --members --verbose
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe
```

For a deterministic outage demonstration run the consumerOutageLagAndCatchUp integration test; it stops only the analytics listener, produces a fact, observes state progression and positive analytics lag, starts the listener and verifies catch-up. `LagMetrics` also supplies replay progress for each instance's configured projection version. It is progress distance, not a percentage estimate or an exactly-once metric.

## Stuck partition / DB failure

Inspect structured logs by consumerGroup/topic/partition/offset. Check DB readiness, connection pool, constraint violations, quarantine and rate of projection failures. Infrastructure failures retry without committing; fix the cause and allow redelivery. Do not clear processed_events to force progress. Known business-order errors are atomically quarantined and do not stall a partition.

## Poison, incompatible version and DLT

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic eventscale.orders.dlt.v1 --from-beginning --isolation-level read_committed --max-messages 1
```

DLT carries original topic/partition/offset, eventId if parseable, category and timestamp, without arbitrary payload/exception secrets. Fetch the source record from the original retained log for authorized investigation. Fix a producer/contract, then decide whether to publish a corrected new event or perform controlled reprocessing in a new projection version. There is no automatic DLT redrive loop. If DLT cannot publish, normalizer transaction aborts and source offset stays uncommitted; restore DLT and broker health. That deliberate availability tradeoff avoids losing poison diagnostics. Unknown type/version requires explicit contract support before replay, not a blind mapper fallback.

## Rebalance

Inspect group members and partition assignments. Two members should cover three partitions disjointly; stopping one reassigns all to the survivor. Expect temporary lag and possible redelivery. Dedup handles the DB/offset boundary. Investigate repeated rebalances via broker connectivity, processing duration and max.poll.interval; never promise no duplicates on member loss.

## Broker unavailable

Health/readiness becomes DOWN. API admissions can still be durably ACCEPTED if PostgreSQL is healthy; publication receipts remain pending. Dispatcher retries the first pending event. Do not interpret request timeout as definite non-delivery; retry with the same eventId. Restore the same broker volume. RF1 does not recover lost disk data. The F4 test pauses its own real Kafka container and asserts failed/unconfirmed producer delivery.

## Replay / projection rebuild

Follow replay.md. New version, new groups, earliest retained log, compare, activate. No public replay endpoint exists. Never run `down -v` against data you intend to replay. The clean-start command below is for a disposable project only.

## Metrics and logs

`GET /actuator/prometheus`: events produced, produce failures, events consumed, duplicates, projection failures, DLT count, quarantine count, processing duration and consumer lag. Names use Micrometer dot naming internally and Prometheus snake_case externally. Tags are fixed consumer/group/category values only, never eventId/orderId/correlationId. Counters describe attempts unless explicitly deduplicated in SQL. Structured projection logs include all trace/log coordinates, without payload bodies.

`GET /actuator/health/readiness`: application readiness + PostgreSQL + Kafka. `GET /actuator/health/liveness`: JVM/application liveness. Dependency loss should fail readiness without forcing a liveness restart loop. KafkaAdmin creates topics before listener startup; Flyway creates schema before projectors. Use `docker compose logs app` for startup/migration failures.

## Disposable clean start

```sh
docker compose down -v
docker compose up -d --build --wait --wait-timeout 180
pwsh ./scripts/smoke.ps1
```

`down -v` deletes only this Compose project's declared volumes; verify project name before running. CI runs in an isolated runner. Local verification uses its own project name, never another application's containers or volumes.
