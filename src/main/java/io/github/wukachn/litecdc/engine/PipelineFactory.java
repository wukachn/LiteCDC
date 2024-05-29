package io.github.wukachn.litecdc.engine;

import io.github.wukachn.litecdc.api.model.request.PipelineConfiguration;
import io.github.wukachn.litecdc.engine.kafka.ChangeEventProducer;
import io.github.wukachn.litecdc.engine.metrics.MetricsService;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PipelineFactory {

  private final MetricsService metricsService;

  public Pipeline create(PipelineConfiguration config) {
    metricsService.initiateTables(config.getSourceConfig().getTables());
    var changeEventProducer =
        new ChangeEventProducer(
            metricsService,
            config.getKafkaConfig().getBootstrapServer(),
            config.getKafkaConfig().getTopicPrefix(),
            config.getKafkaConfig().getTopicStrategy());
    var snapshotter = config.getSourceConfig().getSnapshotter(changeEventProducer, metricsService);
    var streamer = config.getSourceConfig().getStreamer(changeEventProducer, metricsService);
    EmailHandler emailHandler = null;
    if (config.getEmailConfig() != null) {
      emailHandler = config.getEmailConfig().getEmailHandler();
    }
    return Pipeline.builder()
        .pipelineConfiguration(config)
        .snapshotter(snapshotter)
        .streamer(streamer)
        .metricsService(metricsService)
        .emailHandler(emailHandler)
        .build();
  }
}
