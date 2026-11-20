package io.eventscale.intake;

import io.eventscale.kafka.Topology;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name="eventscale.dispatch-enabled",havingValue="true",matchIfMissing=true)
public class Publisher {
    private final JdbcTemplate db; private final TransactionTemplate tx;
    private final KafkaTemplate<String,String> kafka; private final MeterRegistry metrics;
    public Publisher(JdbcTemplate db, TransactionTemplate tx, @Qualifier("plainTemplate") KafkaTemplate<String,String> kafka, MeterRegistry metrics) {
        this.db=db; this.tx=tx; this.kafka=kafka; this.metrics=metrics;
    }
    @Scheduled(fixedDelayString="${eventscale.dispatch-delay:200}")
    public void tick() {
        try { publishNext(); }
        catch (Exception e) { metrics.counter("eventscale.produce.failures").increment(); LoggerFactory.getLogger(getClass()).warn("Publish deferred: {}", e.getClass().getSimpleName()); }
    }
    public boolean publishNext() {
        return Boolean.TRUE.equals(tx.execute(status -> {
            // One durable dispatcher order across instances. Do not skip a failed earlier record.
            if (!Boolean.TRUE.equals(db.queryForObject("SELECT pg_try_advisory_xact_lock(48291001)", Boolean.class))) return false;
            var rows = db.queryForList("SELECT sequence, aggregate_id, content FROM intake WHERE published_at IS NULL ORDER BY sequence LIMIT 1 FOR UPDATE");
            if (rows.isEmpty()) return false;
            var row = rows.getFirst();
            try {
                var result = kafka.send(Topology.ORDERS, row.get("aggregate_id").toString(), row.get("content").toString()).get(12, TimeUnit.SECONDS);
                var m = result.getRecordMetadata();
                db.update("UPDATE intake SET published_at=current_timestamp, partition_id=?, kafka_offset=? WHERE sequence=?", m.partition(), m.offset(), row.get("sequence"));
                metrics.counter("eventscale.events.produced").increment();
                return true;
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Publish interrupted", e); }
            catch (Exception e) { throw new IllegalStateException("Publication unconfirmed; intake retained", e); }
        }));
    }
}
