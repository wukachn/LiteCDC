package com.thirdyearproject.changedatacaptureapplication.util;

import com.thirdyearproject.changedatacaptureapplication.engine.exception.EnvironmentVariableNotFoundException;

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
