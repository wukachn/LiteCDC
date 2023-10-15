package com.thirdyearproject.changedatacaptureapplication.engine.snapshot;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class PostgresSnapshotInfo {
  @NonNull String snapshotName;
  @NonNull String walStartLsn;
}
