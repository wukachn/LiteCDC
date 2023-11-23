package com.thirdyearproject.changedatacaptureapplication.engine.snapshot;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.postgresql.replication.LogSequenceNumber;

@Value
@Builder
public class PostgresSnapshotInfo {
  @NonNull String snapshotName;
  @NonNull LogSequenceNumber walStartLsn;
}
