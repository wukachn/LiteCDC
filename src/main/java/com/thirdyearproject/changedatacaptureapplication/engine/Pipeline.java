package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.streaming.Streamer;
import java.io.Closeable;
import java.io.IOException;
import lombok.Builder;

@Builder
public class Pipeline implements Closeable, Runnable {
  PipelineConfiguration pipelineConfiguration;
  Snapshotter snapshotter;
  Streamer streamer;

  @Override
  public void close() throws IOException {}

  @Override
  public void run() {
    snapshotter.lockingSnapshot(pipelineConfiguration.getDatabaseConfig().getTables());
  }
}
