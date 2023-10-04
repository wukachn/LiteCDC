package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.PipelineConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PipelineInitializer {

  private final PipelineFactory pipelineFactory;

  public PipelineInitializer() {
    this.pipelineFactory = new PipelineFactory();
  }

  public void runPipeline(PipelineConfiguration config) {
    var pipeline = pipelineFactory.create(config);

    log.info("Starting Pipeline");
    new Thread(pipeline).start();
  }
}
