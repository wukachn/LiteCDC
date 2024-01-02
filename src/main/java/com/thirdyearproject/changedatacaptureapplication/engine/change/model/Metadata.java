package com.thirdyearproject.changedatacaptureapplication.engine.change.model;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public abstract class Metadata {
  @Getter private TableIdentifier tableId;
  @Getter private CRUD op;

  public abstract String getOffset();
}
