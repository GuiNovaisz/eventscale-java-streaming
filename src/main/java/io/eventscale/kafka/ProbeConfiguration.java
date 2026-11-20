package io.eventscale.kafka;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
@Configuration
public class ProbeConfiguration {
    @Bean @ConditionalOnMissingBean(FailureProbe.class) FailureProbe failureProbe() { return new FailureProbe() {}; }
}
