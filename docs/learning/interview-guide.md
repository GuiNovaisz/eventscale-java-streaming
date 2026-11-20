# Interview discussion guide

Use code and executed evidence to support each answer, and identify what the tests do not prove.

1. Why does this problem need a retained log rather than a reliable work queue? Point to two independently maintained offsets.
2. Trace one eventId from API acceptance through raw Kafka, normalization, state, analytics and replay. Which identifiers are business identity versus transport coordinates?
3. Explain why acks=all on RF1 is not HA. What failures would require replication and more controllers?
4. Draw the crash window between PostgreSQL commit and Kafka offset commit. Locate the SQL constraint that makes redelivery safe.
5. Draw the different transaction boundary for the normalizer. Explain output/offset atomicity and read_committed, including transaction markers.
6. Explain why EOS does not collapse two separate input records with the same eventId or include PostgreSQL effects.
7. Describe same-key ordering, concurrent upstream producers, wrong business order and partition expansion risks.
8. Demonstrate rebalance assignments and partition parallelism using the latch and group evidence, without relying on throughput claims.
9. Explain state quarantine versus independent fact analytics. Why is the analytics revenue not a financial ledger?
10. Walk through a non-destructive rebuild and activation. What happens after retention expires? Why must the new process remain running?
11. Show v1/v2 compatibility and unsupported-version behavior. Which forward-compatibility claim would be misleading?
12. Explain deserializer/header-mapper safety and the current advisories checked before pinning versions.
13. Diagnose lag with and without projection failures. How do transaction marker offsets affect a naive lag calculation?
14. Distinguish controlled exception injection from proving process-kill, power-loss or disk durability. What additional experiments would be legitimate future work?

Avoid claiming global order, universal exactly-once delivery, automatic schema governance, unlimited replay history or benchmark results that were not measured.
