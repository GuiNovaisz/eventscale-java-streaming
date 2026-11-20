package io.eventscale.kafka;

import io.eventscale.events.Contracts;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class Normalizer {
    private final Contracts contracts; private final KafkaTemplate<String,String> kafka;
    private final MeterRegistry metrics; private final FailureProbe probe;
    public Normalizer(Contracts contracts, @Qualifier("transactionalTemplate") KafkaTemplate<String,String> kafka, MeterRegistry metrics, FailureProbe probe) {
        this.contracts=contracts; this.kafka=kafka; this.metrics=metrics; this.probe=probe;
    }
    @KafkaListener(id="normalizer", topics=Topology.ORDERS, groupId="eventscale-normalizer-v1", containerFactory="eosFactory", autoStartup="${eventscale.normalizer-enabled:true}")
    public void normalize(ConsumerRecord<String,String> record) throws Exception {
        try {
            var e = contracts.decode(record.value());
            if (!e.aggregateId().equals(record.key())) throw new Contracts.InvalidEvent("KEY_MISMATCH");
            var output = new ProducerRecord<String,String>(Topology.NORMALIZED, record.key(), contracts.normalize(e));
            output.headers().add("correlationId", e.correlationId().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            kafka.send(output).get(12, TimeUnit.SECONDS);
            probe.afterKafkaWrite(e.eventId());
        } catch (Contracts.InvalidEvent invalid) {
            // Invalid contracts are deterministic: one attempt, DLT and input offset in the same transaction.
            var json = JsonMapper.builder().build();
            String eventId="unparseable";
            try { eventId=json.readTree(record.value()).path("eventId").asText("unparseable"); } catch (Exception ignored) { }
            String dlt=json.writeValueAsString(Map.of("originalTopic",record.topic(),"partition",record.partition(),"offset",record.offset(),
                    "eventId",eventId,"failureCategory",invalid.getMessage(),"timestamp",Instant.now().toString()));
            probe.beforeDeadLetter(record.key());
            try { kafka.send(Topology.DLT, record.key(), dlt).get(12, TimeUnit.SECONDS); }
            catch (Exception failure) { probe.afterDeadLetterFailure(record.key()); throw failure; }
            metrics.counter("eventscale.dlt.count", "category", invalid.getMessage()).increment();
        }
    }
}
