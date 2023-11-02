package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PipelineFactory {
  private final ChangeEventProducer changeEventProducer;

  public Pipeline create(PipelineConfiguration config) {
    var snapshotter = config.getDatabaseConfig().getSnapshotter();
    var streamer = config.getDatabaseConfig().getStreamer();
    return Pipeline.builder()
        .pipelineConfiguration(config)
        .snapshotter(snapshotter)
        .streamer(streamer)
        .changeEventProducer(changeEventProducer)
        .build();
  }
}
