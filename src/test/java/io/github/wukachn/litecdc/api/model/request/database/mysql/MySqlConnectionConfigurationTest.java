package io.github.wukachn.litecdc.api.model.request.database.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import org.junit.Test;

public class MySqlConnectionConfigurationTest {

  @Test
  public void buildsValidJdbcUrl() {
    var additionalJdbc = new HashMap<String, String>();
    additionalJdbc.put("ignoreMe", "true");

    var connectionConfig =
        MySqlConnectionConfiguration.builder()
            .host("localhost")
            .port(3306)
            .user("mysql_user")
            .password("MYSQL_PASS")
            .jdbcProperties(additionalJdbc)
            .build();
    assertEquals("jdbc:mysql://localhost:3306", connectionConfig.getJdbcUrl());
  }

  @Test
  public void correctDefaultParameters() {
    var connectionConfig =
        MySqlConnectionConfiguration.builder()
            .host("localhost")
            .port(3306)
            .user("mysql_user")
            .password("MYSQL_PASS")
            .build();

    var properties = connectionConfig.getJdbcProperties();
    assertEquals(3, properties.size());
    assertEquals("mysql_user", properties.getProperty("user"));
    assertEquals("mysql_password", properties.getProperty("password"));
    assertEquals("true", properties.getProperty("allowMultiQueries"));
  }

  @Test
  public void userDefinedJdbcDoNotOverrideApplicationSet() {
    var additionalJdbc = new HashMap<String, String>();
    additionalJdbc.put("user", "diff_user");
    additionalJdbc.put("password", "IGNORE");
    additionalJdbc.put("allowMultiQueries", "false");
    additionalJdbc.put("newJdbcProp", "NEW");

    var connectionConfig =
        MySqlConnectionConfiguration.builder()
            .host("localhost")
            .port(3306)
            .user("mysql_user")
            .password("MYSQL_PASS")
            .jdbcProperties(additionalJdbc)
            .build();

    var properties = connectionConfig.getJdbcProperties();
    assertEquals(4, properties.size());
    assertEquals("mysql_user", properties.getProperty("user"));
    assertEquals("mysql_password", properties.getProperty("password"));
    assertEquals("true", properties.getProperty("allowMultiQueries"));
    assertEquals("NEW", properties.getProperty("newJdbcProp"));
  }
}
