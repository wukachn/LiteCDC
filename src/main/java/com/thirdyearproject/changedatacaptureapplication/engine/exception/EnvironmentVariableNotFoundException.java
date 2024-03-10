package com.thirdyearproject.changedatacaptureapplication.engine.exception;

public class EnvironmentVariableNotFoundException extends RuntimeException {
  public EnvironmentVariableNotFoundException(String name) {
    super(String.format("Couldn't find environment variable with the name: %s", name));
  }
}
