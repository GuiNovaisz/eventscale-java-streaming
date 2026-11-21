package io.eventscale.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class LagMetrics {
    private final KafkaAdmin kafka; private final Map<String,AtomicLong> gauges=new LinkedHashMap<>();
    public LagMetrics(KafkaAdmin kafka,MeterRegistry metrics,@Value("${eventscale.projection-version:v1}") String version) {
        this.kafka=kafka;
        for(String group:List.of("eventscale-normalizer-v1","order-state-projector-"+version,"analytics-projector-"+version)) {
            AtomicLong value=new AtomicLong(-1); gauges.put(group,value);
            metrics.gauge("eventscale.consumer.lag",List.of(io.micrometer.core.instrument.Tag.of("group",group)),value);
        }
    }
    @Scheduled(fixedDelayString="${eventscale.lag-delay:10000}")
    public void sample() {
        try(var admin=Admin.create(kafka.getConfigurationProperties())) {
            for(var entry:gauges.entrySet()) {
                String topic=entry.getKey().equals("eventscale-normalizer-v1")?Topology.ORDERS:Topology.NORMALIZED;
                Map<TopicPartition,OffsetSpec> requests=new HashMap<>();
                for(int i=0;i<3;i++) requests.put(new TopicPartition(topic,i),OffsetSpec.latest());
                var end=admin.listOffsets(requests).all().get(3,TimeUnit.SECONDS);
                var committed=admin.listConsumerGroupOffsets(entry.getKey()).partitionsToOffsetAndMetadata().get(3,TimeUnit.SECONDS);
                long lag=0;
                for(var p:end.entrySet()) lag+=Math.max(0,p.getValue().offset()-(committed.containsKey(p.getKey())?committed.get(p.getKey()).offset():0));
                entry.getValue().set(lag);
            }
        } catch(Exception unavailable) { gauges.values().forEach(g -> g.set(-1)); }
    }
}
