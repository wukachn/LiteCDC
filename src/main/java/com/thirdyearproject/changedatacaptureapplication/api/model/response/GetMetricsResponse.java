package com.thirdyearproject.changedatacaptureapplication.api.model.response;

import com.thirdyearproject.changedatacaptureapplication.engine.metrics.TableCRUD;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class GetMetricsResponse {
  long pipelineStartTime;
  List<TableCRUD> tables;
}
