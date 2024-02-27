package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.KafkaProducerService;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import java.time.Instant;
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
    changeEvent.getMetadata().setProducedTime(Instant.now().toEpochMilli());
    metricsService.produceEvent(changeEvent);
    kafkaProducerService.sendEvent(changeEvent, changeEvent.getMetadata().getTableId());
  }
}
