package io.eventscale.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
@EnableKafka
public class Topology {
    public static final String ORDERS="eventscale.orders.v1";
    public static final String NORMALIZED="eventscale.order-events-normalized.v1";
    public static final String DLT="eventscale.orders.dlt.v1";
    @Bean KafkaAdmin.NewTopics topics() { return new KafkaAdmin.NewTopics(topic(ORDERS), topic(NORMALIZED), topic(DLT)); }
    private NewTopic topic(String name) {
        return new NewTopic(name, 3, (short)1).configs(Map.of("retention.ms", "604800000", "cleanup.policy", "delete", "min.insync.replicas", "1", "max.message.bytes", "131072", "max.decompressed.message.bytes", "131072", "message.timestamp.type", "LogAppendTime"));
    }
    public static Map<String,Object> producerProperties(String brokers) {
        var p = new HashMap<String,Object>();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.ACKS_CONFIG, "all"); p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        p.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        p.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 131072);
        return p;
    }
    public static Map<String,Object> consumerProperties(String brokers) {
        var p = new HashMap<String,Object>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        p.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        return p;
    }
    @Bean("plainFactory") DefaultKafkaProducerFactory<String,String> plainFactory(@Value("${spring.kafka.bootstrap-servers}") String brokers) { return new DefaultKafkaProducerFactory<>(producerProperties(brokers)); }
    @Bean("plainTemplate") KafkaTemplate<String,String> plainTemplate(@Qualifier("plainFactory") ProducerFactory<String,String> factory) { return new KafkaTemplate<>(factory); }
    @Bean("transactionalFactory") DefaultKafkaProducerFactory<String,String> transactionalFactory(@Value("${spring.kafka.bootstrap-servers}") String brokers,
            @Value("${eventscale.instance-id:}") String instance) {
        var factory = new DefaultKafkaProducerFactory<String,String>(producerProperties(brokers));
        factory.setTransactionIdPrefix("eventscale-" + (instance.isBlank() ? UUID.randomUUID() : instance) + "-");
        return factory;
    }
    @Bean("transactionalTemplate") KafkaTemplate<String,String> transactionalTemplate(@Qualifier("transactionalFactory") ProducerFactory<String,String> factory) { return new KafkaTemplate<>(factory); }
    @Bean KafkaTransactionManager<String,String> kafkaTransactionManager(@Qualifier("transactionalFactory") ProducerFactory<String,String> factory) { return new KafkaTransactionManager<>(factory); }
    @Bean JdbcTransactionManager databaseTransactionManager(DataSource ds) { return new JdbcTransactionManager(ds); }
    @Bean TransactionTemplate transactionTemplate(JdbcTransactionManager manager) { return new TransactionTemplate(manager); }
    private ConcurrentKafkaListenerContainerFactory<String,String> base(String brokers) {
        var f = new ConcurrentKafkaListenerContainerFactory<String,String>();
        f.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProperties(brokers)));
        var converter = new org.springframework.kafka.support.converter.MessagingMessageConverter();
        converter.setHeaderMapper(new org.springframework.kafka.support.SimpleKafkaHeaderMapper());
        f.setRecordMessageConverter(converter);
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        f.getContainerProperties().setSyncCommits(true);
        f.getContainerProperties().setFixTxOffsets(true);
        f.setConcurrency(2);
        return f;
    }
    @Bean("projectionFactory") ConcurrentKafkaListenerContainerFactory<String,String> projectionFactory(@Value("${spring.kafka.bootstrap-servers}") String brokers) {
        var f = base(brokers);
        // Infrastructure faults must not be discarded or mistaken for poison events.
        f.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(500, FixedBackOff.UNLIMITED_ATTEMPTS)));
        return f;
    }
    @Bean("eosFactory") ConcurrentKafkaListenerContainerFactory<String,String> eosFactory(@Value("${spring.kafka.bootstrap-servers}") String brokers, KafkaTransactionManager<String,String> manager) {
        var f = base(brokers); f.getContainerProperties().setKafkaAwareTransactionManager(manager); return f;
    }
}
