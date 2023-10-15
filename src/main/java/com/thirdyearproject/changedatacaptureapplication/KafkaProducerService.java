package com.thirdyearproject.changedatacaptureapplication;

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaProducerService {
  private final KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendMessage(String msg) {
    CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("test", msg);
    future.whenComplete(
        (result, ex) -> {
          if (ex == null) {
            log.info(
                "Sent message=["
                    + msg
                    + "] with offset=["
                    + result.getRecordMetadata().offset()
                    + "]");
          } else {
            log.error("Unable to send message=[" + msg + "] due to : " + ex.getMessage());
          }
        });
  }
}
