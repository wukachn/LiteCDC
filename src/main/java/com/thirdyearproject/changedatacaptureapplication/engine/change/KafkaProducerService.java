package com.thirdyearproject.changedatacaptureapplication.engine.change;

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaProducerService {
  private final KafkaTemplate<String, GenericRecord> kafkaTemplate;

  @Value(value = "${spring.kafka.topic-prefix}")
  private String prefix;

  @Autowired
  public KafkaProducerService(KafkaTemplate<String, GenericRecord> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  // Currently using auto.create.topics.enable = true [locally], no need to handle topic creation
  public void sendEvent(GenericRecord genericRecord, String tableId) {
    var topic = String.format("%s.%s", prefix, tableId);

    CompletableFuture<SendResult<String, GenericRecord>> future =
        kafkaTemplate.send(topic, genericRecord);

    future.whenComplete(
        (result, ex) -> {
          if (ex == null) {
            log.info(
                "Sent message=["
                    + genericRecord
                    + "] with offset=["
                    + result.getRecordMetadata().offset()
                    + "] with topic=["
                    + result.getRecordMetadata().topic()
                    + "]");
          } else {
            log.error("Unable to send message=[" + genericRecord + "] due to : " + ex.getMessage());
          }
        });
  }
}
