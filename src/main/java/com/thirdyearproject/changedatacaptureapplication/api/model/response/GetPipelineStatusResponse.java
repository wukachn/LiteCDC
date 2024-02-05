package com.thirdyearproject.changedatacaptureapplication.api.model.response;

import com.thirdyearproject.changedatacaptureapplication.engine.metrics.PipelineStatus;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class GetPipelineStatusResponse {
  PipelineStatus status;
}
