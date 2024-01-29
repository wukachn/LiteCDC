package com.thirdyearproject.changedatacaptureapplication.engine;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PgOutputColumnMetadata {
  boolean isNullable;
  int size;
}
