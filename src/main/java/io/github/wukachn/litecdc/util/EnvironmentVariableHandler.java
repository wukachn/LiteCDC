package io.github.wukachn.litecdc.util;

import io.github.wukachn.litecdc.engine.exception.EnvironmentVariableNotFoundException;

public class EnvironmentVariableHandler {
  public static String get(String name) {
    var value = System.getenv(name);
    if (value != null) {
      return value;
    } else {
      throw new EnvironmentVariableNotFoundException(name);
    }
  }
}
