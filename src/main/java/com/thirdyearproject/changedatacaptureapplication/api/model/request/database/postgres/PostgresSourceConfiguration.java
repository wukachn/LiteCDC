package com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.SourceConfiguration;
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
import org.springframework.lang.Nullable;

@Value
@Builder
@Jacksonized
@Slf4j
public class PostgresSourceConfiguration implements SourceConfiguration {
  @NonNull PostgresConnectionConfiguration connectionConfig;
  @NonNull Set<TableIdentifier> capturedTables;
  @Nullable String replicationSlot;
  @Nullable String publication;

  private String getReplicationSlot() {
    if (replicationSlot != null) {
      return replicationSlot;
    }
    return "cdc_replication_slot";
  }

  private String getPublication() {
    if (publication != null) {
      return publication;
    }
    return "cdc_publication";
  }

  @Override
  public Set<TableIdentifier> getTables() {
    return capturedTables;
  }

  @Override
  public Snapshotter getSnapshotter() {
    return new PostgresSnapshotter(connectionConfig, getPublication(), getReplicationSlot());
  }

  @Override
  public Streamer getStreamer() {
    return new PostgresStreamer(connectionConfig, getPublication(), getReplicationSlot());
  }
}
