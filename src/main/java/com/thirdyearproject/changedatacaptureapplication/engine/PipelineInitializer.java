package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PipelineInitializer {

  private final PipelineFactory pipelineFactory;
  private Thread pipelineThread;

  public PipelineInitializer(ChangeEventProducer changeEventProducer, MetricsService metricsServer) {
    this.pipelineFactory = new PipelineFactory(changeEventProducer, metricsServer);
  }

  public synchronized void runPipeline(PipelineConfiguration config) {
    log.info("Attempting to start pipeline.");
    if (pipelineThread != null) {
      log.error("A pipeline is already running.");
      throw new PipelineConflictException("A pipeline is already running.");
    }
    var pipeline = pipelineFactory.create(config);
    pipelineThread = new Thread(pipeline);
    pipelineThread.start();
    log.info("Pipeline started.");
  }

  public synchronized void haltPipeline() {
    log.info("Attempting to halt pipeline.");
    if (pipelineThread == null) {
      log.error("No pipeline is running.");
      throw new PipelineConflictException("No pipeline is running.");
    }
    pipelineThread.interrupt();
    try {
      pipelineThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    pipelineThread = null;
    log.info("Pipeline halted.");
  }
}
