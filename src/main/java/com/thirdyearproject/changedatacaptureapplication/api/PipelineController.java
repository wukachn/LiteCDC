package com.thirdyearproject.changedatacaptureapplication.api;

import com.thirdyearproject.changedatacaptureapplication.api.model.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.PipelineInitializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/pipeline")
public class PipelineController {

  private final PipelineInitializer pipelineInitializer;

  @Autowired
  public PipelineController(PipelineInitializer pipelineInitializer) {
    this.pipelineInitializer = pipelineInitializer;
  }

  @PostMapping("/run")
  public void runPipeline(@RequestBody PipelineConfiguration config) {
    pipelineInitializer.runPipeline(config);
  }

  @PostMapping("/halt")
  public void haltPipeline() {
    pipelineInitializer.haltPipeline();
  }
}
