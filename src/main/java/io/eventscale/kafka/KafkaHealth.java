package io.eventscale.kafka;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.admin.Admin;
import java.util.concurrent.TimeUnit;

@Component("kafka")
public class KafkaHealth implements HealthIndicator {
    private final KafkaAdmin admin;
    public KafkaHealth(KafkaAdmin admin) { this.admin=admin; }
    public Health health() {
        try (var client=Admin.create(admin.getConfigurationProperties())) {
            client.describeCluster().nodes().get(3,TimeUnit.SECONDS);
            return Health.up().build();
        } catch(Exception e) { return Health.down().build(); }
    }
}
