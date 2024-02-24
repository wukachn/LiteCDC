package com.thirdyearproject.changedatacaptureapplication.engine;

public class PipelineNotRunningException extends RuntimeException {
  public PipelineNotRunningException(String msg) {
    super(msg);
  }
}
