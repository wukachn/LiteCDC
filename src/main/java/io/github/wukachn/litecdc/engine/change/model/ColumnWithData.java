package io.github.wukachn.litecdc.engine.change.model;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
public class ColumnWithData {
  ColumnDetails details;
  Object value;
}
