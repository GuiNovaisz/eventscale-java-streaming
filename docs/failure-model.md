# Failure model

Evidence names below are methods in StreamingIT unless noted. Kafka and PostgreSQL are real Testcontainers, no embedded broker or mocked KafkaTemplate.

| Failure | Expected behavior | Evidence | Residual risk |
| --- | --- | --- | --- |
| Producer retry | idempotent protocol retry; durable intake retained if unconfirmed | ordering / intake tests + producer config | restart/ack-DB gap can physically duplicate |
| Broker unavailable | send is unconfirmed, no false published receipt | brokerUnavailableDoesNotProduceFalseConfirmation (real broker paused) | RF1 physical loss not recoverable |
| Duplicate | one SQL effect per version/group/eventId | duplicateDeliveryHasOneLogicalEffect | same fact with a new eventId is a new fact |
| Consumer crash after DB commit | redelivery, dedup prevents second revenue/count | crashAfterDatabaseCommitRedeliversWithoutDoubleRevenue | boundary exception injection, not power-loss testing |
| DB failure | marker/mutation rollback together, record retried | databaseFailureRollsBackDedupAndRetries | prolonged outage grows lag |
| Rebalance | ownership redistributes, no logical loss | twoConsumersDistributePartitionsAndRebalanceWithoutLoss | temporary pause/duplicates expected |
| Poison JSON | one deterministic validation attempt, transactional DLT, next record progresses | poisonAndUnsupportedContractsDoNotBlockPartition | DLT availability is required for progress |
| Unsupported version/type | classified, never treated as v1 | schema test, poison test, unit dispatch | new contracts need explicit rollout |
| DLT unavailable | transaction aborts; source offset does not advance past poison | deadLetterOutageDoesNotCommitPastPoison | partition intentionally stalls until restored |
| Replay | isolated new version/groups yield equivalent state | replayNewGroupRebuildsEquivalentProjectionAfterDeletion | expired history cannot be reconstructed |
| Kafka transaction abort | downstream read_committed hides aborted output | abortedTransactionInvisibleToReadCommitted; transactionalStageRollsBackOutputAndOffsetTogether | external DB not part of Kafka transaction |
| Business out-of-order | quarantine state inconsistency, independent analytics counts fact | schemaV1AndV2AndBusinessDisorder | no automatic reorder/correction |
| Malicious type headers | raw byte mapping, explicit contract only | hostileTypeHeadersCannotSelectClasses | no public broker/HTTP authentication in local scope |

F1=duplicate, F2=crash, F3=poison, F4=broker, F5=outage, F6=rebalance, F7=replay, F8=abort. These are overlapping integration scenarios, not an additional fabricated count of standalone tests.
