package io.github.wukachn.litecdc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.wukachn.litecdc.engine.exception.EnvironmentVariableNotFoundException;
import org.junit.Test;

public class EnvironmentVariableHandlerTest {
  @Test
  public void does_exist_returns() {
    assertEquals("pg_password", EnvironmentVariableHandler.get("PG_PASS"));
  }

  @Test
  public void does_not_exist_throws() {
    assertThrows(
        EnvironmentVariableNotFoundException.class,
        () -> EnvironmentVariableHandler.get("DOES_NOT_EXIST"));
  }
}
