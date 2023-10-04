package com.thirdyearproject.changedatacaptureapplication.api;

import com.thirdyearproject.changedatacaptureapplication.api.model.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.PipelineInitializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class PipelineController {

  private final PipelineInitializer pipelineInitializer;

  @Autowired
  public PipelineController(PipelineInitializer pipelineInitializer) {
    this.pipelineInitializer = pipelineInitializer;
  }

  @GetMapping("/pipeline/run")
  public void runPipeline(@RequestBody PipelineConfiguration config) {
    pipelineInitializer.runPipeline(config);
  }
}
