package com.thirdyearproject.changedatacaptureapplication.api.model.database;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.snapshot.PostgresSnapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.streaming.Streamer;
import java.util.Set;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class PostgresConfiguration implements DatabaseConfiguration {
  @NonNull PostgresConnectionConfiguration connectionConfig;
  @NonNull Set<String> capturedTables;

  @Override
  public Set<String> getTables() {
    return capturedTables;
  }

  @Override
  public Snapshotter getSnapshotter() {
    return new PostgresSnapshotter(new JdbcConnection(connectionConfig));
  }

  @Override
  public Streamer getStreamer() {
    return null;
  }
}
