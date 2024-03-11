package com.thirdyearproject.changedatacaptureapplication.engine.exception;

public class ValidationException extends RuntimeException {
  public ValidationException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
