package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChangeEventProducer {
  private KafkaProducerService kafkaProducerService;

  public ChangeEventProducer(KafkaProducerService kafkaProducerService) {
    this.kafkaProducerService = kafkaProducerService;
  }

  public void sendEvent(ChangeEvent changeEvent) {
    var tableId = changeEvent.getMetadata().getTableId();
    kafkaProducerService.sendEvent(changeEvent, tableId);
  }
}
