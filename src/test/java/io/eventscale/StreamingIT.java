package io.eventscale;

import io.eventscale.events.Contracts;
import io.eventscale.intake.Intake;
import io.eventscale.intake.Publisher;
import io.eventscale.kafka.*;
import io.eventscale.projection.Projector;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT, properties={"eventscale.dispatch-delay=30","eventscale.lag-delay=60000"})
@Import(StreamingIT.Probes.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StreamingIT {
    @Container static KafkaContainer kafka=new KafkaContainer("apache/kafka:4.4.0")
            .withEnv("KAFKA_MAX_DECOMPRESSED_MESSAGE_BYTES","131072");
    @Container static PostgreSQLContainer postgres=new PostgreSQLContainer("postgres:18.7-alpine");
    @DynamicPropertySource static void properties(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers",kafka::getBootstrapServers);
        r.add("spring.datasource.url",postgres::getJdbcUrl); r.add("spring.datasource.username",postgres::getUsername); r.add("spring.datasource.password",postgres::getPassword);
    }
    @Autowired Intake intake; @Autowired Publisher publisher; @Autowired JdbcTemplate db;
    @Autowired Projector projector; @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired Contracts contracts; @Autowired TestProbe probe;
    @Autowired @Qualifier("plainTemplate") KafkaTemplate<String,String> template;
    @Value("${local.server.port}") int port;
    @TestConfiguration static class Probes { @Bean @Primary TestProbe testProbe() { return new TestProbe(); } }
    static class TestProbe implements FailureProbe {
        volatile UUID crashId; volatile UUID abortId; volatile UUID slowId;
        final AtomicInteger crashAttempts=new AtomicInteger(); final AtomicInteger abortAttempts=new AtomicInteger();
        volatile CountDownLatch entered=new CountDownLatch(0), release=new CountDownLatch(0);
        volatile CountDownLatch dltAttempt=new CountDownLatch(1);
        public void beforeDeadLetter(String key) { if ("dlt-unavailable".equals(key)) dltAttempt.countDown(); }
        public void afterDatabaseCommit(String consumer,UUID id) {
            if (consumer.equals("analytics") && id.equals(crashId) && crashAttempts.getAndIncrement()==0) throw new IllegalStateException("TEST_CRASH_AFTER_DB_COMMIT");
        }
        public void afterKafkaWrite(UUID id) { if(id.equals(abortId) && abortAttempts.getAndIncrement()==0) throw new IllegalStateException("TEST_ABORT_AFTER_WRITE"); }
        public void beforeProjection(String consumer,UUID id) {
            if(consumer.equals("state") && id.equals(slowId)) {
                entered.countDown();
                try { if(!release.await(15,TimeUnit.SECONDS)) throw new IllegalStateException("Test barrier timed out"); }
                catch(InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
            }
        }
    }
    void eventually(Runnable assertion) { await().atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofMillis(100)).untilAsserted(assertion::run); }
    int count(String sql,Object...args) { return db.queryForObject(sql,Integer.class,args); }
    void projected(UUID id) { eventually(()->assertThat(count("SELECT count(*) FROM processed_events WHERE version='v1' AND event_id=?",id)).isEqualTo(2)); }
    String status(String order) { var rows=db.queryForList("SELECT status FROM order_state WHERE version='v1' AND order_id=?",order); return rows.isEmpty()?"missing":rows.getFirst().get("status").toString(); }
    RecordMetadata publish(String key,String json) throws Exception { return template.send(Topology.ORDERS,key,json).get(15,TimeUnit.SECONDS).getRecordMetadata(); }
    KafkaConsumer<String,String> reader(String group) { var p=Topology.consumerProperties(kafka.getBootstrapServers()); p.put(ConsumerConfig.GROUP_ID_CONFIG,group); return new KafkaConsumer<>(p); }
    List<ConsumerRecord<String,String>> readAll(String topic) {
        try(var c=reader("proof-"+UUID.randomUUID())) {
            var partitions=IntStream.range(0,3).mapToObj(i->new TopicPartition(topic,i)).toList(); c.assign(partitions); c.seekToBeginning(partitions);
            var end=c.endOffsets(partitions); List<ConsumerRecord<String,String>> result=new ArrayList<>();
            await().atMost(Duration.ofSeconds(20)).until(()-> { c.poll(Duration.ofMillis(100)).forEach(result::add); return partitions.stream().allMatch(p->c.position(p)>=end.get(p)); });
            return result;
        }
    }
    @Test @Order(1) void intakeStorageIdempotencyAndHttpLimits() throws Exception {
        UUID id=UUID.randomUUID(); String body=Samples.event(id,"intake-order","order.created",1);
        var client=HttpClient.newHttpClient();
        var uri=URI.create("http://localhost:"+port+"/api/v1/events");
        java.util.function.Function<String,HttpResponse<String>> post=b -> { try { return client.send(HttpRequest.newBuilder(uri).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(b)).build(),HttpResponse.BodyHandlers.ofString()); } catch(Exception e) { throw new IllegalStateException(e); } };
        assertThat(post.apply(body).statusCode()).isEqualTo(202);
        assertThat(post.apply(body).statusCode()).isEqualTo(202);
        assertThat(post.apply(body.replace("customer-1","other")).statusCode()).isEqualTo(409);
        assertThat(post.apply("{bad").statusCode()).isEqualTo(400);
        assertThat(post.apply("x".repeat(65537)).statusCode()).isEqualTo(413);
        projected(id); assertThat(count("SELECT count(*) FROM intake WHERE event_id=?",id)).isEqualTo(1);
    }
    @Test @Order(2) void orderingKeyOffsetsAndIndependentGroups() throws Exception {
        String order="ordered"; List<UUID> ids=new ArrayList<>(); List<RecordMetadata> metadata=new ArrayList<>();
        for(String type:List.of("order.created","payment.confirmed","order.shipped","order.delivered")) {
            UUID id=UUID.randomUUID(); ids.add(id); metadata.add(publish(order,Samples.event(id,order,type,1)));
        }
        ids.forEach(this::projected);
        assertThat(metadata.stream().map(RecordMetadata::partition).distinct().count()).isEqualTo(1);
        assertThat(metadata.stream().map(RecordMetadata::offset).toList()).isSorted().doesNotHaveDuplicates();
        var records=readAll(Topology.ORDERS).stream().filter(r->r.key().equals(order)).toList();
        assertThat(records).extracting(ConsumerRecord::key).containsOnly(order);
        assertThat(records.stream().map(r->contracts.decode(r.value()).eventId()).toList()).containsExactlyElementsOf(ids);
        assertThat(status(order)).isEqualTo("DELIVERED");
        System.out.println("ORDERING_PROOF key="+order+" partition="+metadata.getFirst().partition()+" offsets="+metadata.stream().map(RecordMetadata::offset).toList()+" groups=state,analytics");
    }
    @Test @Order(3) void duplicateDeliveryHasOneLogicalEffect() throws Exception {
        String order="duplicate"; UUID id=UUID.randomUUID(); String json=Samples.event(id,order,"order.created",1);
        publish(order,json); publish(order,json); projected(id);
        eventually(()->assertThat(count("SELECT count(*) FROM order_state WHERE order_id=?",order)).isEqualTo(1));
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id=?",id)).isEqualTo(2);
        UUID payment=UUID.randomUUID(); String paid=Samples.event(payment,order,"payment.confirmed",1).replace("BRL","XAA");
        publish(order,paid); publish(order,paid); projected(payment);
        eventually(()->assertThat(db.queryForObject("SELECT revenue FROM analytics WHERE version='v1' AND event_type='payment.confirmed' AND currency='XAA'",java.math.BigDecimal.class)).isEqualByComparingTo("42.50"));
        assertThat(count("SELECT event_count FROM analytics WHERE version='v1' AND event_type='payment.confirmed' AND currency='XAA'")).isEqualTo(1);
    }
    @Test @Order(4) void crashAfterDatabaseCommitRedeliversWithoutDoubleRevenue() throws Exception {
        String order="crash"; UUID created=UUID.randomUUID(); publish(order,Samples.event(created,order,"order.created",1)); projected(created);
        UUID id=UUID.randomUUID(); probe.crashId=id;
        publish(order,Samples.event(id,order,"payment.confirmed",1).replace("BRL","XAB"));
        eventually(()->assertThat(probe.crashAttempts.get()).isGreaterThanOrEqualTo(2)); projected(id);
        assertThat(db.queryForObject("SELECT revenue FROM analytics WHERE version='v1' AND currency='XAB'",java.math.BigDecimal.class)).isEqualByComparingTo("42.50");
        assertThat(count("SELECT event_count FROM analytics WHERE version='v1' AND currency='XAB'")).isEqualTo(1);
        System.out.println("CRASH_PROOF attempts="+probe.crashAttempts+" revenue=42.50 count=1");
    }
    @Test @Order(5) void poisonAndUnsupportedContractsDoNotBlockPartition() throws Exception {
        String key="poison";
        var bad=publish(key,"{broken");
        var version=publish(key,Samples.event(UUID.randomUUID(),key,"order.created",999));
        var unknown=publish(key,Samples.event(UUID.randomUUID(),key,"unknown",1));
        UUID good=UUID.randomUUID(); var next=publish(key,Samples.event(good,key,"order.created",1)); projected(good);
        assertThat(next.partition()).isEqualTo(bad.partition());
        eventually(()-> {
            var errors=readAll(Topology.DLT).stream().map(r->r.value()).toList();
            assertThat(errors).anyMatch(s->s.contains("MALFORMED_JSON") && s.contains("\"offset\":"+bad.offset()));
            assertThat(errors).anyMatch(s->s.contains("UNSUPPORTED_VERSION") && s.contains("\"offset\":"+version.offset()));
            assertThat(errors).anyMatch(s->s.contains("UNKNOWN_TYPE") && s.contains("\"offset\":"+unknown.offset()));
        });
    }
    @Test @Order(6) void transactionalStageRollsBackOutputAndOffsetTogether() throws Exception {
        UUID id=UUID.randomUUID(); probe.abortId=id; publish("abort-stage",Samples.event(id,"abort-stage","order.created",1));
        eventually(()->assertThat(probe.abortAttempts.get()).isGreaterThanOrEqualTo(2)); projected(id);
        var visible=readAll(Topology.NORMALIZED).stream().filter(r->r.value().contains(id.toString())).toList();
        assertThat(visible).hasSize(1);
        System.out.println("EOS_STAGE_PROOF attempts="+probe.abortAttempts+" read_committed_outputs="+visible.size());
    }
    @Test @Order(7) void abortedTransactionInvisibleToReadCommitted() throws Exception {
        UUID aborted=UUID.randomUUID(),committed=UUID.randomUUID();
        var props=Topology.producerProperties(kafka.getBootstrapServers()); props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"proof-"+UUID.randomUUID());
        try(var p=new KafkaProducer<String,String>(props)) {
            p.initTransactions(); p.beginTransaction(); p.send(new ProducerRecord<>(Topology.NORMALIZED,"tx-abort",Samples.event(aborted,"tx-abort","order.created",1))).get(); p.abortTransaction();
            p.beginTransaction(); p.send(new ProducerRecord<>(Topology.NORMALIZED,"tx-commit",Samples.event(committed,"tx-commit","order.created",1))).get(); p.commitTransaction();
        }
        projected(committed);
        var visible=readAll(Topology.NORMALIZED);
        assertThat(visible).noneMatch(r->r.value().contains(aborted.toString())).anyMatch(r->r.value().contains(committed.toString()));
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id=?",aborted)).isZero();
    }
    @Test @Order(8) void schemaV1AndV2AndBusinessDisorder() throws Exception {
        UUID v1=UUID.randomUUID(),v2=UUID.randomUUID(),disorder=UUID.randomUUID();
        publish("schema1",Samples.event(v1,"schema1","order.created",1)); publish("schema2",Samples.event(v2,"schema2","order.created",2));
        publish("disorder",Samples.event(disorder,"disorder","order.shipped",1));
        projected(v1); projected(v2); projected(disorder);
        assertThat(db.queryForObject("SELECT channel FROM order_state WHERE order_id='schema1'",String.class)).isEqualTo("unknown");
        assertThat(db.queryForObject("SELECT channel FROM order_state WHERE order_id='schema2'",String.class)).isEqualTo("web");
        assertThat(count("SELECT count(*) FROM quarantine WHERE event_id=? AND category='BUSINESS_ORDER_INCONSISTENCY'",disorder)).isEqualTo(1);
        assertThat(status("disorder")).isEqualTo("missing");
    }
    @Test @Order(9) void consumerOutageLagAndCatchUp() throws Exception {
        var container=registry.getListenerContainer("analytics"); container.stop();
        UUID id=UUID.randomUUID(); publish("backlog",Samples.event(id,"backlog","order.created",1));
        eventually(()->assertThat(status("backlog")).isEqualTo("CREATED"));
        assertThat(count("SELECT count(*) FROM processed_events WHERE event_id=? AND consumer_name='analytics'",id)).isZero();
        try(var admin=Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,kafka.getBootstrapServers()))) {
            eventually(()->assertThat(lag(admin,"analytics-projector-v1")).isPositive());
            container.start(); projected(id); eventually(()->assertThat(lag(admin,"analytics-projector-v1")).isZero());
        } finally { container.start(); }
    }
    long lag(Admin admin,String group) {
        try {
        var committed=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        Map<TopicPartition,OffsetSpec> req=new HashMap<>(); for(int i=0;i<3;i++) req.put(new TopicPartition(Topology.NORMALIZED,i),OffsetSpec.latest());
        var end=admin.listOffsets(req).all().get(); long sum=0;
        for(var e:end.entrySet()) sum+=Math.max(0,e.getValue().offset()-(committed.containsKey(e.getKey())?committed.get(e.getKey()).offset():0)); return sum;
        } catch(Exception e) { throw new IllegalStateException(e); }
    }
    @Test @Order(10) void twoConsumersDistributePartitionsAndRebalanceWithoutLoss() throws Exception {
        var container=(ConcurrentMessageListenerContainer<?,?>)registry.getListenerContainer("state");
        eventually(()->assertThat(container.getContainers().stream().filter(c->!c.getAssignedPartitions().isEmpty()).count()).isEqualTo(2));
        var assigned=container.getContainers().stream().flatMap(c->c.getAssignedPartitions().stream()).toList();
        assertThat(assigned).hasSize(3).doesNotHaveDuplicates();
        System.out.println("GROUP_PROOF assignments="+container.getContainers().stream().map(c->c.getAssignedPartitions().toString()).toList());
        var child=container.getContainers().getFirst(); child.stop();
        List<UUID> ids=new ArrayList<>(); for(int i=0;i<12;i++) { UUID id=UUID.randomUUID(); ids.add(id); publish("rebalance-"+i,Samples.event(id,"rebalance-"+i,"order.created",1)); }
        try { eventually(()->assertThat(container.getContainers().get(1).getAssignedPartitions()).hasSize(3)); ids.forEach(this::projected); }
        finally { child.start(); }
        eventually(()->assertThat(container.getContainers().stream().filter(c->!c.getAssignedPartitions().isEmpty()).count()).isEqualTo(2));
    }
    @Test @Order(11) void concurrentAggregatesUseMultiplePartitionsAndParallelConsumers() throws Exception {
        probe.slowId=UUID.randomUUID(); probe.entered=new CountDownLatch(1); probe.release=new CountDownLatch(1);
        String blocked="parallel-blocked"; int partition=publish(blocked,Samples.event(probe.slowId,blocked,"order.created",1)).partition();
        assertThat(probe.entered.await(15,TimeUnit.SECONDS)).isTrue();
        var container=(ConcurrentMessageListenerContainer<?,?>)registry.getListenerContainer("state");
        var blockedChild=container.getContainers().stream().filter(c->c.getAssignedPartitions().contains(new TopicPartition(Topology.NORMALIZED,partition))).findFirst().orElseThrow();
        Set<Integer> other=container.getContainers().stream().filter(c->c!=blockedChild).flatMap(c->c.getAssignedPartitions().stream()).map(TopicPartition::partition).collect(Collectors.toSet());
        var executor=Executors.newFixedThreadPool(3); List<Future<Map.Entry<UUID,RecordMetadata>>> futures=new ArrayList<>();
        long start=System.nanoTime();
        try {
            for(int i=0;i<18;i++) { final int n=i; futures.add(executor.submit(()-> {
                String key="parallel-"+n; UUID last=null; List<RecordMetadata> sequence=new ArrayList<>();
                for(String type:List.of("order.created","payment.confirmed","order.shipped","order.delivered")) {
                    last=UUID.randomUUID(); sequence.add(publish(key,Samples.event(last,key,type,1)));
                }
                assertThat(sequence.stream().map(RecordMetadata::partition).distinct().count()).isEqualTo(1);
                assertThat(sequence.stream().map(RecordMetadata::offset).toList()).isSorted().doesNotHaveDuplicates();
                return Map.entry(last,sequence.getLast());
            })); }
            List<Map.Entry<UUID,RecordMetadata>> results=new ArrayList<>(); for(var f:futures) results.add(f.get(20,TimeUnit.SECONDS));
            assertThat(results.stream().map(e->e.getValue().partition()).distinct().count()).isGreaterThan(1);
            var unblocked=results.stream().filter(e->other.contains(e.getValue().partition())).findFirst().orElseThrow(); projected(unblocked.getKey());
            assertThat(count("SELECT count(*) FROM processed_events WHERE event_id=? AND consumer_name='state'",probe.slowId)).isZero();
            probe.release.countDown(); results.forEach(e->projected(e.getKey())); projected(probe.slowId);
            for(int i=0;i<18;i++) assertThat(status("parallel-"+i)).isEqualTo("DELIVERED");
            System.out.println("CONTROLLED_SMOKE events=73 partitions=3 stateConsumers=2 durationMs="+TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start));
        } finally { probe.release.countDown(); executor.shutdownNow(); }
    }
    @Test @Order(12) void brokerUnavailableDoesNotProduceFalseConfirmation() {
        var props=Topology.producerProperties(kafka.getBootstrapServers()); props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,500); props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,1000); props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,500);
        try(var producer=new KafkaProducer<String,String>(props)) {
            producer.partitionsFor(Topology.ORDERS);
            kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();
            try {
            assertThatThrownBy(()->producer.send(new ProducerRecord<>(Topology.ORDERS,"offline",Samples.created("offline"))).get(3,TimeUnit.SECONDS)).isInstanceOf(Exception.class);
            } finally { kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec(); }
        }
    }
    @Test @Order(13) void databaseFailureRollsBackDedupAndRetries() throws Exception {
        String key="db-failure"; UUID id=UUID.randomUUID();
        // A transactional CHECK fault is deterministic and limited to this event, while PostgreSQL remains real.
        db.execute("ALTER TABLE order_state ADD CONSTRAINT test_db_fault CHECK (order_id <> 'db-failure')");
        try {
            publish(key,Samples.event(id,key,"order.created",1));
            eventually(()->assertThat(count("SELECT count(*) FROM processed_events WHERE event_id=? AND consumer_name='analytics'",id)).isEqualTo(1));
            assertThat(count("SELECT count(*) FROM processed_events WHERE event_id=? AND consumer_name='state'",id)).isZero();
        } finally { db.execute("ALTER TABLE order_state DROP CONSTRAINT test_db_fault"); }
        projected(id); assertThat(status(key)).isEqualTo("CREATED");
    }
    @Test @Order(14) void hostileTypeHeadersCannotSelectClasses() throws Exception {
        UUID id=UUID.randomUUID();
        var record=new ProducerRecord<String,String>(Topology.ORDERS,"hostile",Samples.event(id,"hostile","order.created",1));
        record.headers().add("spring.kafka.serialization.selector","java.lang.Runtime".getBytes());
        record.headers().add("spring_json_header_types","{\"danger\":\"java.lang.Class\"}".getBytes());
        record.headers().add("danger","\"does.not.exist.Unsafe\"".getBytes());
        template.send(record).get(15,TimeUnit.SECONDS); projected(id);
        assertThat(status("hostile")).isEqualTo("CREATED");
    }
    @Test @Order(16) void deadLetterOutageDoesNotCommitPastPoison() throws Exception {
        String key="dlt-unavailable"; UUID id=UUID.randomUUID();
        try(var admin=Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,kafka.getBootstrapServers()))) {
            admin.deleteTopics(List.of(Topology.DLT)).all().get(10,TimeUnit.SECONDS);
            try {
                var poison=publish(key,"{broken"); publish(key,Samples.event(id,key,"order.created",1));
                assertThat(probe.dltAttempt.await(15,TimeUnit.SECONDS)).isTrue();
                var offsets=admin.listConsumerGroupOffsets("eventscale-normalizer-v1").partitionsToOffsetAndMetadata().get();
                var offset=offsets.get(new TopicPartition(Topology.ORDERS,poison.partition()));
                assertThat(offset==null?0:offset.offset()).isLessThanOrEqualTo(poison.offset());
                assertThat(count("SELECT count(*) FROM processed_events WHERE event_id=?",id)).isZero();
            } finally {
                admin.createTopics(List.of(new NewTopic(Topology.DLT,3,(short)1).configs(Map.of("retention.ms","604800000","cleanup.policy","delete","min.insync.replicas","1","message.timestamp.type","LogAppendTime","max.message.bytes","131072","max.decompressed.message.bytes","131072")))).all().get(10,TimeUnit.SECONDS);
            }
            projected(id);
        }
    }
    @Test @Order(100) void replayNewGroupRebuildsEquivalentProjectionAfterDeletion() {
        registry.getListenerContainer("state").stop(); registry.getListenerContainer("analytics").stop();
        try {
            String version="rebuild"; db.update("INSERT INTO projection_versions(version) VALUES (?)",version);
            var input=readAll(Topology.NORMALIZED);
            input.forEach(r->{projector.process("state",version,r);projector.process("analytics",version,r);});
            var original=stateSnapshot(version); var analytics=analyticsSnapshot(version);
            assertThat(original).isEqualTo(stateSnapshot("v1")); assertThat(analytics).isEqualTo(analyticsSnapshot("v1"));
            db.update("DELETE FROM order_state WHERE version=?",version); db.update("DELETE FROM analytics WHERE version=?",version); db.update("DELETE FROM processed_events WHERE version=?",version); db.update("DELETE FROM quarantine WHERE version=?",version);
            try(var c=reader("replay-rebuild-"+UUID.randomUUID())) {
                c.subscribe(List.of(Topology.NORMALIZED));
                await().atMost(Duration.ofSeconds(30)).until(()-> {
                    for(var r:c.poll(Duration.ofMillis(200))) {projector.process("state",version,r);projector.process("analytics",version,r);}
                    if(c.assignment().isEmpty()) return false;
                    var end=c.endOffsets(c.assignment());
                    boolean done=c.assignment().stream().allMatch(p->c.position(p)>=end.get(p));
                    c.commitSync(); return done;
                });
            }
            assertThat(stateSnapshot(version)).isEqualTo(original); assertThat(analyticsSnapshot(version)).isEqualTo(analytics);
            System.out.println("REPLAY_PROOF orders="+original.size()+" analyticsRows="+analytics.size()+" equal=true newConsumerGroup=true");
        } finally {registry.getListenerContainer("state").start();registry.getListenerContainer("analytics").start();}
    }
    List<Map<String,Object>> stateSnapshot(String v) { return db.queryForList("SELECT order_id,status,last_event_id,event_time,channel FROM order_state WHERE version=? ORDER BY order_id",v); }
    List<Map<String,Object>> analyticsSnapshot(String v) { return db.queryForList("SELECT event_type,currency,event_count,revenue FROM analytics WHERE version=? ORDER BY event_type,currency",v); }
}
