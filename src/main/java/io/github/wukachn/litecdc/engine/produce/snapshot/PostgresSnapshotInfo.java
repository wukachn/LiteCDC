package io.github.wukachn.litecdc.engine.produce.snapshot;

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
