package com.thirdyearproject.changedatacaptureapplication.api.model.response;

import com.thirdyearproject.changedatacaptureapplication.engine.metrics.TableRowsSnapshot;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class GetSnapshotMetricsResponse {
  boolean completed;
  long durationSeconds;
  List<TableRowsSnapshot> tables;
}
