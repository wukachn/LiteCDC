package io.github.wukachn.litecdc.api;

import io.github.wukachn.litecdc.api.model.request.PipelineConfiguration;
import io.github.wukachn.litecdc.api.model.response.GetMetricsResponse;
import io.github.wukachn.litecdc.api.model.response.GetPipelineStatusResponse;
import io.github.wukachn.litecdc.api.model.response.GetSnapshotMetricsResponse;
import io.github.wukachn.litecdc.engine.PipelineInitializer;
import io.github.wukachn.litecdc.engine.exception.PipelineConflictException;
import io.github.wukachn.litecdc.engine.exception.PipelineNotRunningException;
import io.github.wukachn.litecdc.engine.exception.ValidationException;
import io.github.wukachn.litecdc.engine.metrics.MetricsService;
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
  public void runPipeline(@RequestBody PipelineConfiguration config)
      throws PipelineConflictException, ValidationException {
    PipelineInitializer.runPipeline(config, metricsService);
  }

  @PostMapping("/halt")
  public void haltPipeline() throws PipelineNotRunningException {
    PipelineInitializer.haltPipeline();
  }

  @GetMapping("/status")
  public GetPipelineStatusResponse getPipelineStatus() {
    return metricsService.getPipelineStatus();
  }

  @GetMapping("/metrics/snapshot")
  public GetSnapshotMetricsResponse getSnapshotMetrics() throws PipelineNotRunningException {
    return metricsService.getSnapshotMetrics();
  }

  @GetMapping("/metrics")
  public GetMetricsResponse getMetrics() throws PipelineNotRunningException {
    return metricsService.getMetrics();
  }
}
