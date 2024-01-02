package com.thirdyearproject.changedatacaptureapplication.engine.change.model;

import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.postgresql.replication.LogSequenceNumber;

@SuperBuilder
public class PostgresMetadata extends Metadata {

  @Getter private LogSequenceNumber lsn;

  @Override
  public String getOffset() {
    return String.valueOf(lsn.asLong());
  }
}
