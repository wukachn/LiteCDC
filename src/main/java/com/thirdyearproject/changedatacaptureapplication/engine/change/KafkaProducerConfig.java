package com.thirdyearproject.changedatacaptureapplication.engine.change;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaProducerConfig {

  @Value(value = "${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;

  // To create messages, we first need to configure a ProducerFactory. This sets the strategy for
  // creating Kafka Producer instances.
  // (https://www.baeldung.com/spring-kafka)
  @Bean
  public ProducerFactory<String, GenericRecord> producerFactory() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    configProps.put("schema.registry.url", "http://schema-registry:8081");
    configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    return new DefaultKafkaProducerFactory<>(configProps);
  }

  // Then we need a KafkaTemplate, which wraps a Producer instance and provides convenience methods
  // for sending messages to Kafka topics.
  // (https://www.baeldung.com/spring-kafka)
  @Bean
  public KafkaTemplate<String, GenericRecord> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }
}
