package io.github.wukachn.litecdc.api.model.request.database.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import org.junit.Test;

public class PostgresConnectionConfigurationTest {

  @Test
  public void buildsValidJdbcUrl() {
    var additionalJdbc = new HashMap<String, String>();
    additionalJdbc.put("ignoreMe", "true");

    var connectionConfig =
        PostgresConnectionConfiguration.builder()
            .host("localhost")
            .port(5432)
            .user("pg_user")
            .password("PG_PASS")
            .database("pg_db")
            .jdbcProperties(additionalJdbc)
            .build();
    assertEquals("jdbc:postgresql://localhost:5432/pg_db", connectionConfig.getJdbcUrl());
  }

  @Test
  public void correctDefaultParameters() {
    var connectionConfig =
        PostgresConnectionConfiguration.builder()
            .host("localhost")
            .port(5432)
            .user("pg_user")
            .password("PG_PASS")
            .database("pg_db")
            .build();

    var properties = connectionConfig.getJdbcProperties();
    assertEquals(3, properties.size());
    assertEquals("pg_user", properties.getProperty("user"));
    assertEquals("pg_password", properties.getProperty("password"));
    assertEquals("9.4", properties.getProperty("assumeMinServerVersion"));
  }

  @Test
  public void userDefinedJdbcDoNotOverrideApplicationSet() {
    var additionalJdbc = new HashMap<String, String>();
    additionalJdbc.put("user", "diff_user");
    additionalJdbc.put("password", "IGNORE");
    additionalJdbc.put("assumeMinServerVersion", "1.1");
    additionalJdbc.put("newJdbcProp", "NEW");

    var connectionConfig =
        PostgresConnectionConfiguration.builder()
            .host("localhost")
            .port(5432)
            .user("pg_user")
            .password("PG_PASS")
            .database("pg_db")
            .jdbcProperties(additionalJdbc)
            .build();

    var properties = connectionConfig.getJdbcProperties();
    assertEquals(4, properties.size());
    assertEquals("pg_user", properties.getProperty("user"));
    assertEquals("pg_password", properties.getProperty("password"));
    assertEquals("9.4", properties.getProperty("assumeMinServerVersion"));
    assertEquals("NEW", properties.getProperty("newJdbcProp"));
  }
}
