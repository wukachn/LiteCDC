package com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.ConnectionConfiguration;
import java.util.Map;
import java.util.Properties;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;

@Slf4j
@Value
@Builder
@Jacksonized
public class PostgresConnectionConfiguration implements ConnectionConfiguration {
  @NonNull String host;
  @NonNull Integer port;
  @NonNull String database;
  @NonNull String user;
  @NonNull String password;
  @Nullable Map<String, String> jdbcProperties;

  @Override
  @JsonIgnore
  public String getJdbcUrl() {
    return "jdbc:postgresql://" + host + ":" + port + "/" + database;
  }

  @Override
  @JsonIgnore
  public Properties getJdbcProperties() {
    var properties = new Properties();
    if (jdbcProperties != null) {
      jdbcProperties.forEach(properties::setProperty);
    }
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    // Exported snapshots Minimum Version: Postgres 9.4+
    properties.setProperty("assumeMinServerVersion", "9.4");
    return properties;
  }
}
