package io.github.wukachn.litecdc.api.model.response;

import io.github.wukachn.litecdc.engine.metrics.PipelineStatus;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class GetPipelineStatusResponse {
  PipelineStatus status;
}
