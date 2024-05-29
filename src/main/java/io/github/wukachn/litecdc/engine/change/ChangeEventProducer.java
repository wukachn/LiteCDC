package io.github.wukachn.litecdc.engine.change;

import io.github.wukachn.litecdc.api.model.request.TopicStrategy;
import io.github.wukachn.litecdc.engine.change.model.ChangeEvent;
import io.github.wukachn.litecdc.engine.kafka.KafkaProducerService;
import io.github.wukachn.litecdc.engine.metrics.MetricsService;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChangeEventProducer {
  private final KafkaProducerService kafkaProducerService;
  private final MetricsService metricsService;
  private TopicStrategy topicStrategy;

  public ChangeEventProducer(
      MetricsService metricsService,
      String bootstrapAddress,
      String topicPrefix,
      TopicStrategy topicStrategy) {
    this.kafkaProducerService = new KafkaProducerService(bootstrapAddress, topicPrefix);
    this.metricsService = metricsService;
    this.topicStrategy = topicStrategy;
  }

  public void sendEvent(ChangeEvent changeEvent) {
    changeEvent.getMetadata().setProducedTime(Instant.now().toEpochMilli());
    kafkaProducerService.sendEvent(changeEvent, topicStrategy);
    metricsService.produceEvent(changeEvent);
  }
}
