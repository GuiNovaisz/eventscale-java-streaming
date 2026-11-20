# Partitioning and ordering

All three application topics have 3 partitions, RF=1, minISR=1, delete cleanup and 604800000 ms retention. Broker auto-create is disabled; `Topology` declares topics and safety limits. Topic partitions must remain fixed for this milestone. Default producer partitioning hashes the aggregate key. Normalizer preserves that key and topic partition count.

Kafka orders log offsets **within a partition**, not globally across partitions. Same aggregate key maps to the same partition under this fixed topology. Independent aggregates can share a partition or run on different partitions. No ordering claim is based on wall-clock timestamp or global offset comparison. Concurrent requests are admitted in a serialized local order; Kafka does not fix an upstream shipment sent before payment.

State and analytics each have a distinct group and concurrency=2. The three partitions distribute across those two members; within each group a partition has one active owner. Groups independently see the same log. A member leaving triggers reassignment; redelivery around rebalance is tolerated by DB dedup. More than three active members cannot add partition parallelism here.

The ordering proof publishes created/paid/shipped/delivered under one key, checks one partition and strictly increasing offsets, and verifies DELIVERED. The parallel proof blocks one state consumer behind a latch while another assigned consumer commits other keys. The group proof prints disjoint assignments, removes one member, observes all three assignments on the survivor, and checks every event's persisted effects.

Scaling the dispatcher would require per-key scheduling with durable sequence ownership; this milestone deliberately uses a single logical dispatcher. Scaling consumers demonstrates Kafka partition parallelism without pretending intake is an unlimited-throughput service.
