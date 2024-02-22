package com.thirdyearproject.changedatacaptureapplication.engine.metrics;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TableRowsSnapshot {
  TableIdentifier table;
  long rows;
  boolean completed;
}
