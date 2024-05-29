package io.github.wukachn.litecdc.engine.produce.streaming;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PgOutputColumnMetadata {
  boolean isNullable;
  int size;
}
