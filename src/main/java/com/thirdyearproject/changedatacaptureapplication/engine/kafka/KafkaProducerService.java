package com.thirdyearproject.changedatacaptureapplication.engine.kafka;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.TopicStrategy;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.serialization.ChangeEventSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
public class KafkaProducerService {
  private final KafkaTemplate<String, ChangeEvent> kafkaTemplate;

  private String topicPrefix;

  public KafkaProducerService(String bootstrapAddress, String topicPrefix) {
    this.kafkaTemplate = kafkaTemplate(bootstrapAddress);
    this.topicPrefix = topicPrefix;
  }

  public void sendEvent(ChangeEvent changeEvent, TopicStrategy topicStrategy) {
    String topic;
    if (topicStrategy == TopicStrategy.PER_TABLE) {
      topic =
          String.format("%s.%s", topicPrefix, changeEvent.getMetadata().getTableId().getStringFormat());
    } else {
      topic = String.format("%s.all_tables", topicPrefix);
    }

    CompletableFuture<SendResult<String, ChangeEvent>> future =
        kafkaTemplate.send(topic, changeEvent);

    future.whenComplete(
        (result, ex) -> {
          if (ex == null) {

            log.info(
                "Sent message=["
                    + changeEvent
                    + "] with offset=["
                    + result.getRecordMetadata().offset()
                    + "] with topic=["
                    + result.getRecordMetadata().topic()
                    + "]");
          } else {
            log.error("Unable to send message=[" + changeEvent + "] due to : " + ex.getMessage());
          }
        });
  }

  private KafkaTemplate<String, ChangeEvent> kafkaTemplate(String bootstrapAddress) {
    return new KafkaTemplate<>(producerFactory(bootstrapAddress));
  }

  private ProducerFactory<String, ChangeEvent> producerFactory(String bootstrapAddress) {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ChangeEventSerializer.class);
    return new DefaultKafkaProducerFactory<>(configProps);
  }
}
