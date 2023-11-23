package com.thirdyearproject.changedatacaptureapplication.engine.temp;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Column {
  String name;
  int type;
  boolean isNullable;
}
