package com.thirdyearproject.changedatacaptureapplication.api.model.database.postgres;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.SourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot.PostgresSnapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming.PostgresStreamer;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming.Streamer;
import java.util.Set;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Jacksonized
@Slf4j
public class PostgresSourceConfiguration implements SourceConfiguration {
  @NonNull PostgresConnectionConfiguration connectionConfig;
  @NonNull Set<TableIdentifier> capturedTables;

  @Override
  public Set<TableIdentifier> getTables() {
    return capturedTables;
  }

  @Override
  public Snapshotter getSnapshotter() {
    return new PostgresSnapshotter(connectionConfig);
  }

  @Override
  public Streamer getStreamer() {
    return new PostgresStreamer(connectionConfig);
  }
}
