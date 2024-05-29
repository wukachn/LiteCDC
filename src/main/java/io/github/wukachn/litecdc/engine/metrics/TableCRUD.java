package io.github.wukachn.litecdc.engine.metrics;

import io.github.wukachn.litecdc.engine.change.model.TableIdentifier;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TableCRUD {
  TableIdentifier table;
  CrudCount operationCounts;
}
