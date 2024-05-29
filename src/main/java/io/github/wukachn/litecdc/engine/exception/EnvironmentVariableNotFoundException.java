package io.github.wukachn.litecdc.engine.exception;

public class EnvironmentVariableNotFoundException extends RuntimeException {
  public EnvironmentVariableNotFoundException(String name) {
    super(String.format("Couldn't find environment variable with the name: %s", name));
  }
}
