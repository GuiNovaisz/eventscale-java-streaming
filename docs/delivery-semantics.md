# Delivery and transaction boundaries

| Boundary | Guarantee | Duplicate / failure behavior |
| --- | --- | --- |
| HTTP → intake | durable unique eventId | same canonical content returns same logical receipt; conflict=409 |
| intake → Kafka | at-least-once publication | DB commit/ack gap can duplicate physical records |
| raw → normalized/DLT | Kafka transaction/EOS | output and consumed offset commit together; rollback redelivers |
| normalized → PostgreSQL | at-least-once consumption, idempotent DB effect | processed marker and mutation share local DB transaction |

Producer configuration: acks=all, enable.idempotence=true, retries=Integer.MAX_VALUE, max.in.flight=5, delivery.timeout.ms=10000, request.timeout.ms=3000, max.block.ms=5000. Retries are bounded by delivery timeout. Timeout means unconfirmed publication, not proof that no broker ever appended the record. API receipt remains ACCEPTED until confirmed; client retry must preserve eventId.

Kafka producer idempotence suppresses certain retry duplicates within a producer session. Business event identity survives process restarts and independent producers. These are different guarantees.

Normalizer uses Spring Kafka's transactional listener container, transactional KafkaTemplate and unique per-instance transactional ID prefix. The container sends consumed offsets to the same transaction as output. read_committed hides aborted writes. Deterministic validation failures go straight to a metadata-only DLT in that same transaction. Infrastructure/DLT publication errors abort and retain input, with rollback/retry; a DLT outage intentionally stalls that partition until recovery. Do not commit/drop errors to hide lag. Metrics count processing attempts and can include aborted attempts.

Projectors explicitly disable auto-commit and use synchronous RECORD acknowledgements. Listener returns only after TransactionTemplate commits PostgreSQL. On exception the container seeks/retries, and does not acknowledge the failed record. A crash between DB commit and offset commit redelivers. UNIQUE(version,consumer_name,event_id) plus mutation in one transaction prevents a second effect. A conflicting reused ID is quarantined. DB infrastructure failures roll back marker and effect together and retry indefinitely rather than being discarded as poison.

**Kafka-native read-process-write uses Kafka transactions/EOS. PostgreSQL projections remain idempotent under at-least-once consumption. No Kafka+PostgreSQL distributed exactly-once transaction is claimed.** Read/process callbacks and metrics can run multiple times. EOS does not deduplicate two distinct input log entries that happen to share an eventId.

`StreamingIT` places a deterministic failure after a real DB commit and before listener return. It also throws after transactional output to prove offset/output rollback, and publishes explicit committed/aborted transactions to prove downstream visibility. This is controlled boundary failure injection, not an OS power-loss or disk-corruption proof.
