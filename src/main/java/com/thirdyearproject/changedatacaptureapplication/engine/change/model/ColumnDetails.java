package com.thirdyearproject.changedatacaptureapplication.engine.change.model;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Jacksonized
@Slf4j
public class ColumnDetails {
  String name;
  int sqlType;
  boolean isNullable;
  boolean isPrimaryKey;
  int size;
}
