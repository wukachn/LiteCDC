package com.thirdyearproject.changedatacaptureapplication.api;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetMetricsResponse;
import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetPipelineStatusResponse;
import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetSnapshotMetricsResponse;
import com.thirdyearproject.changedatacaptureapplication.engine.PipelineInitializer;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/pipeline")
public class PipelineController {
  private final MetricsService metricsService;

  @Autowired
  public PipelineController(MetricsService metricsService) {
    this.metricsService = metricsService;
  }

  @PostMapping("/run")
  public void runPipeline(@RequestBody PipelineConfiguration config) {
    PipelineInitializer.runPipeline(config, metricsService);
  }

  @PostMapping("/halt")
  public void haltPipeline() {
    PipelineInitializer.haltPipeline();
  }

  @GetMapping("/status")
  public GetPipelineStatusResponse getPipelineStatus() {
    return metricsService.getPipelineStatus();
  }

  @GetMapping("/metrics/snapshot")
  public GetSnapshotMetricsResponse getSnapshotMetrics() {
    return metricsService.getSnapshotMetrics();
  }

  @GetMapping("/metrics")
  public GetMetricsResponse getMetrics() {
    return metricsService.getMetrics();
  }
}
