package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.KafkaProducerService;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChangeEventProducer {
  private KafkaProducerService kafkaProducerService;
  private MetricsService metricsService;

  public ChangeEventProducer(
      KafkaProducerService kafkaProducerService, MetricsService metricsService) {
    this.kafkaProducerService = kafkaProducerService;
    this.metricsService = metricsService;
  }

  public void sendEvent(ChangeEvent changeEvent) {
    var tableId = changeEvent.getMetadata().getTableId();
    var op = changeEvent.getMetadata().getOp();
    metricsService.trackEvent(tableId, op);
    kafkaProducerService.sendEvent(changeEvent, tableId);
  }
}
