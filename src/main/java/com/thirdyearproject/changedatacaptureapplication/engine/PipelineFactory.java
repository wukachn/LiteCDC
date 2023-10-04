package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.PipelineConfiguration;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class PipelineFactory {

  public Pipeline create(PipelineConfiguration config) {
    var snapshotter = config.getDatabaseConfig().getSnapshotter();
    var streamer = config.getDatabaseConfig().getStreamer();
    return Pipeline.builder()
        .pipelineConfiguration(config)
        .snapshotter(snapshotter)
        .streamer(streamer)
        .build();
  }
}
