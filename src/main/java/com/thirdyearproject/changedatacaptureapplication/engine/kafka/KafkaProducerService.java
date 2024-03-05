package com.thirdyearproject.changedatacaptureapplication.engine.kafka;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.TopicStrategy;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
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

  public void sendEvent(ChangeEvent changeEvent, TopicStrategy topicStrategy) {
    String topic;
    if (topicStrategy == TopicStrategy.PER_TABLE) {
      topic =
          String.format("%s.%s", prefix, changeEvent.getMetadata().getTableId().getStringFormat());
    } else {
      topic = String.format("%s.all_tables", prefix);
    }

    CompletableFuture<SendResult<String, ChangeEvent>> future =
        kafkaTemplate.send(topic, changeEvent);

    future.whenComplete(
        (result, ex) -> {
          if (ex == null) {
            /*
            log.info(
                "Sent message=["
                    + changeEvent
                    + "] with offset=["
                    + result.getRecordMetadata().offset()
                    + "] with topic=["
                    + result.getRecordMetadata().topic()
                    + "]");*/
          } else {
            log.error("Unable to send message=[" + changeEvent + "] due to : " + ex.getMessage());
          }
        });
  }
}
