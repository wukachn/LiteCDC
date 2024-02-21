package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PipelineFactory {
  private final ChangeEventProducer changeEventProducer;
  private final MetricsService metricsService;

  public Pipeline create(PipelineConfiguration config) {
    var snapshotter = config.getSourceConfig().getSnapshotter(changeEventProducer, metricsService);
    var streamer = config.getSourceConfig().getStreamer(changeEventProducer, metricsService);
    return Pipeline.builder()
        .pipelineConfiguration(config)
        .snapshotter(snapshotter)
        .streamer(streamer)
        .metricsService(metricsService)
        .build();
  }
}
