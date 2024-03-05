package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.TopicStrategy;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.KafkaProducerService;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChangeEventProducer {
  private final KafkaProducerService kafkaProducerService;
  private final MetricsService metricsService;
  private TopicStrategy topicStrategy;

  public ChangeEventProducer(
      KafkaProducerService kafkaProducerService, MetricsService metricsService) {
    this.kafkaProducerService = kafkaProducerService;
    this.metricsService = metricsService;
  }

  public void sendEvent(ChangeEvent changeEvent) {
    changeEvent.getMetadata().setProducedTime(Instant.now().toEpochMilli());
    metricsService.produceEvent(changeEvent);
    kafkaProducerService.sendEvent(changeEvent, topicStrategy);
  }

  public void setTopicStrategy(TopicStrategy topicStrategy) {
    this.topicStrategy = topicStrategy;
  }
}
