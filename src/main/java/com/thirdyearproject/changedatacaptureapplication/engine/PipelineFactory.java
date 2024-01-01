package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PipelineFactory {
  private final ChangeEventProducer changeEventProducer;

  public Pipeline create(PipelineConfiguration config) {
    var snapshotter = config.getSourceConfig().getSnapshotter();
    var streamer = config.getSourceConfig().getStreamer();
    return Pipeline.builder()
        .pipelineConfiguration(config)
        .snapshotter(snapshotter)
        .streamer(streamer)
        .changeEventProducer(changeEventProducer)
        .build();
  }
}
