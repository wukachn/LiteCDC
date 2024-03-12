package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.PipelineConflictException;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.PipelineNotRunningException;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.ValidationException;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PipelineInitializer {
  private static Thread pipelineThread;

  public static synchronized void runPipeline(
      PipelineConfiguration config, MetricsService metricsService)
      throws PipelineConflictException, ValidationException {
    if (pipelineThread != null && pipelineThread.getState() == Thread.State.TERMINATED) {
      pipelineThread = null;
    }

    if (pipelineThread != null) {
      log.error("A pipeline is already running.");
      throw new PipelineConflictException("A pipeline is already running.");
    }

    log.info("Attempting to start pipeline.");
    config.validate();
    var pipeline = new PipelineFactory(metricsService).create(config);
    pipelineThread = new Thread(pipeline);
    pipelineThread.start();
    log.info("Pipeline started.");
  }

  public static synchronized void haltPipeline() throws PipelineNotRunningException {
    log.info("Attempting to close pipeline.");
    if (pipelineThread == null || pipelineThread.getState() == Thread.State.TERMINATED) {
      log.error("Pipeline not running.");
      throw new PipelineNotRunningException("Pipeline not running.");
    }
    pipelineThread.stop();
    try {
      pipelineThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    pipelineThread = null;
  }
}
