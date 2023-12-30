package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaProducerService {
  private final KafkaTemplate<String, ChangeEvent> kafkaTemplate;

  @Value(value = "${spring.kafka.topic-prefix}")
  private String prefix;

  @Autowired
  public KafkaProducerService(KafkaTemplate<String, ChangeEvent> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  // Currently using auto.create.topics.enable = true [locally], no need to handle topic creation
  public void sendEvent(ChangeEvent changeEvent, TableIdentifier tableId) {
    var topic = String.format("%s.%s", prefix, tableId.getStringFormat());

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
}
