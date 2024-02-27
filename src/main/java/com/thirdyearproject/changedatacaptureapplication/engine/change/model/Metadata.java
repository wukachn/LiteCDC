package com.thirdyearproject.changedatacaptureapplication.engine.change.model;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public abstract class Metadata {
  @Getter private TableIdentifier tableId;
  @Getter private CRUD op;
  @Getter private Long dbCommitTime;
  @Setter @Getter private long producedTime;

  public abstract String getOffset();
}
