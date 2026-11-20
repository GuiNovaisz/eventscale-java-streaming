package io.eventscale.projection;

import io.eventscale.events.Contracts;
import io.eventscale.events.Event;
import io.eventscale.kafka.FailureProbe;
import io.eventscale.kafka.Topology;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.sql.Timestamp;

@Component
public class Projector {
    private final JdbcTemplate db; private final TransactionTemplate tx; private final Contracts contracts;
    private final MeterRegistry metrics; private final FailureProbe probe; private final String version;
    public Projector(JdbcTemplate db, TransactionTemplate tx, Contracts contracts, MeterRegistry metrics, FailureProbe probe,
            @Value("${eventscale.projection-version:v1}") String version) {
        if (!version.matches("[a-zA-Z0-9_-]{1,48}")) throw new IllegalArgumentException("Unsafe projection version");
        this.db=db; this.tx=tx; this.contracts=contracts; this.metrics=metrics; this.probe=probe; this.version=version;
        db.update("INSERT INTO projection_versions(version) VALUES (?) ON CONFLICT DO NOTHING", version);
    }
    @KafkaListener(id="state", topics=Topology.NORMALIZED, groupId="order-state-projector-${eventscale.projection-version:v1}", containerFactory="projectionFactory", autoStartup="${eventscale.projectors-enabled:true}")
    public void state(ConsumerRecord<String,String> record) { process("state",version,record); }
    @KafkaListener(id="analytics", topics=Topology.NORMALIZED, groupId="analytics-projector-${eventscale.projection-version:v1}", containerFactory="projectionFactory", autoStartup="${eventscale.projectors-enabled:true}")
    public void analytics(ConsumerRecord<String,String> record) { process("analytics",version,record); }
    public void process(String consumer, String projectionVersion, ConsumerRecord<String,String> record) {
        Event e = contracts.decode(record.value());
        probe.beforeProjection(consumer,e.eventId());
        var sample=Timer.start(metrics);
        try {
            tx.executeWithoutResult(status -> {
                db.queryForObject("SELECT pg_advisory_xact_lock(hashtextextended(?, 1))",Object.class,projectionVersion+":"+consumer+":"+e.aggregateId());
                String content=contracts.canonical(e);
                int inserted=db.update("INSERT INTO processed_events(version,consumer_name,event_id,content) VALUES (?,?,?,?) ON CONFLICT DO NOTHING",projectionVersion,consumer,e.eventId(),content);
                if (inserted==0) {
                    String original=db.queryForObject("SELECT content FROM processed_events WHERE version=? AND consumer_name=? AND event_id=?",String.class,projectionVersion,consumer,e.eventId());
                    if (!content.equals(original)) quarantine(consumer,projectionVersion,e,record,"EVENT_ID_CONFLICT");
                    else metrics.counter("eventscale.duplicates","consumer",consumer).increment();
                    return;
                }
                if (consumer.equals("state")) stateMutation(projectionVersion,e,record);
                else if (consumer.equals("analytics")) analyticsMutation(projectionVersion,e);
                else throw new IllegalArgumentException("Unknown projection");
            });
            // This boundary is deliberately outside TransactionTemplate: failures redeliver committed work.
            probe.afterDatabaseCommit(consumer,e.eventId());
            metrics.counter("eventscale.events.consumed","consumer",consumer).increment();
            LoggerFactory.getLogger(getClass()).atInfo().addKeyValue("eventId",e.eventId()).addKeyValue("eventType",e.eventType())
                    .addKeyValue("aggregateId",e.aggregateId()).addKeyValue("correlationId",e.correlationId()).addKeyValue("topic",record.topic())
                    .addKeyValue("partition",record.partition()).addKeyValue("offset",record.offset())
                    .addKeyValue("consumerGroup",(consumer.equals("state")?"order-state-projector-":"analytics-projector-")+projectionVersion).log("Projection processed");
        } catch (RuntimeException error) { metrics.counter("eventscale.projection.failures","consumer",consumer).increment(); throw error; }
        finally { sample.stop(metrics.timer("eventscale.processing.duration","consumer",consumer)); }
    }
    private void stateMutation(String v, Event e, ConsumerRecord<String,String> r) {
        var rows=db.queryForList("SELECT status,channel FROM order_state WHERE version=? AND order_id=?",v,e.aggregateId());
        String current=rows.isEmpty()?null:rows.getFirst().get("status").toString();
        String next;
        try { next=Transitions.apply(current,e.eventType()); }
        catch (Transitions.InconsistentEvent invalid) { quarantine("state",v,e,r,"BUSINESS_ORDER_INCONSISTENCY"); return; }
        String channel=rows.isEmpty()?e.payload().path("channel").asText("unknown"):rows.getFirst().get("channel").toString();
        db.update("INSERT INTO order_state(version,order_id,status,last_event_id,event_time,channel) VALUES (?,?,?,?,?,?) ON CONFLICT(version,order_id) DO UPDATE SET status=excluded.status,last_event_id=excluded.last_event_id,event_time=excluded.event_time",v,e.aggregateId(),next,e.eventId(),Timestamp.from(e.occurredAt()),channel);
    }
    private void analyticsMutation(String v, Event e) {
        // Analytics counts facts received, independently of state-machine quarantine; currencies never mix.
        String currency=e.payload().path("currency").asText("---");
        BigDecimal revenue=e.eventType().equals("payment.confirmed")?e.payload().path("total").decimalValue():BigDecimal.ZERO;
        db.update("INSERT INTO analytics(version,event_type,currency,event_count,revenue) VALUES (?,?,?,1,?) ON CONFLICT(version,event_type,currency) DO UPDATE SET event_count=analytics.event_count+1,revenue=analytics.revenue+excluded.revenue",v,e.eventType(),currency,revenue);
    }
    private void quarantine(String c,String v,Event e,ConsumerRecord<String,String> r,String category) {
        db.update("INSERT INTO quarantine(version,consumer_name,event_id,category,topic,partition_id,kafka_offset) VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",v,c,e.eventId(),category,r.topic(),r.partition(),r.offset());
        metrics.counter("eventscale.quarantine","consumer",c,"category",category).increment();
    }
}
