package com.thirdyearproject.changedatacaptureapplication.engine.metrics;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TableCRUD {
  TableIdentifier table;
  CrudCount operationCounts;
}
