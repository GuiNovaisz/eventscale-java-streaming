# Event streaming learning map

Read the root README, then architecture, delivery-semantics, partitioning-ordering and replay. Run the real tests and connect each assertion to a boundary below.

| Concept | Meaning inside EventScale |
| --- | --- |
| broker | Kafka server storing/fetching partition logs; local combined KRaft node is not HA |
| topic | retained logical stream, e.g. orders.v1, containing lifecycle facts rather than one queue per type |
| partition | ordered log shard and unit of group assignment/parallelism |
| offset | location in one partition, not a globally unique business ID |
| consumer group | independent logical subscriber with its own offsets; state and analytics each see the stream |
| rebalance | reassignment when members enter/leave; can pause and redeliver |
| lag | distance between readable log progress and consumer commits, interpreted with transaction markers |
| retention | bounded history availability; seven days here, not permanent event sourcing storage |
| key | aggregateId, routing one order to one partition under fixed partition count |
| idempotent producer | protocol-level retry protection, not business deduplication |
| transactional producer | publishes output and consumed offsets atomically inside Kafka |
| at-least-once | delivery can repeat around faults; logical effects require persistent dedup |
| exactly-once semantics | Kafka-native read-process-write scope, not an automatic PostgreSQL distributed transaction |
| read_committed | skips aborted transactional output and waits behind open transactions |
| replay | reads retained committed history to build a new version of a read model |
| event time | occurredAt from external producer, retained in the envelope; distinct from broker append/DB intake/processing time |
| schema evolution | supported explicit v1/v2 contracts and compatible channel default, not arbitrary future schema acceptance |
| DLT | bounded handling of deterministic invalid records, with source coordinates and classification |

The dispatcher is a reliability handoff, not the reason Kafka exists. The strongest portfolio evidence is independent group progress, ordering boundaries and equivalent reconstruction from retained data. Inspect [interview-guide](interview-guide.md) and complete [exercises](exercises.md) without looking for canned answers.
