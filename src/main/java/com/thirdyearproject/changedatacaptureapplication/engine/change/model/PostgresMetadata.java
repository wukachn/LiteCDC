package com.thirdyearproject.changedatacaptureapplication.engine.change.model;

import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.postgresql.replication.LogSequenceNumber;
import org.springframework.lang.Nullable;

@SuperBuilder
public class PostgresMetadata extends Metadata {

  @NonNull private LogSequenceNumber lsn;
  @Setter @Nullable private LogSequenceNumber commitLsn;
  private Long txId;

  @Override
  public String getOffset() {
    var commitLsnString = String.format("%019d", (commitLsn != null) ? commitLsn.asLong() : 0);
    var lsnString = String.format("%019d", lsn.asLong());
    return String.format("%s~%s", commitLsnString, lsnString);
  }
}
