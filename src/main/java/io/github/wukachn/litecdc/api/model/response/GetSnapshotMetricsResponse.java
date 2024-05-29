package io.github.wukachn.litecdc.api.model.response;

import io.github.wukachn.litecdc.engine.metrics.TableRowsSnapshot;
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
