package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.streaming.Streamer;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import lombok.Builder;

@Builder
public class Pipeline implements Closeable, Runnable {
  PipelineConfiguration pipelineConfiguration;
  Snapshotter snapshotter;
  Streamer streamer;
  ChangeEventProducer changeEventProducer;

  @Override
  public void close() throws IOException {}

  @Override
  public void run() {
    snapshotter.snapshot(
        pipelineConfiguration.getDatabaseConfig().getTables(), changeEventProducer);
    try {
      streamer.stream();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
