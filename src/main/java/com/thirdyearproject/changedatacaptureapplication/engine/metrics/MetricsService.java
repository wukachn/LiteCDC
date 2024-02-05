package com.thirdyearproject.changedatacaptureapplication.engine.metrics;

import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetPipelineStatusResponse;
import lombok.Setter;
import org.springframework.stereotype.Service;

@Service
public class MetricsService {

  @Setter
  PipelineStatus pipelineStatus = PipelineStatus.NOT_RUNNING;

  public GetPipelineStatusResponse getPipelineStatus() {
    return GetPipelineStatusResponse.builder().status(pipelineStatus).build();
  }
}
