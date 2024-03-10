package com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.ConnectionConfiguration;
import java.util.Map;
import java.util.Properties;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;

@Value
@Builder
@Jacksonized
@Slf4j
public class MySqlConnectionConfiguration implements ConnectionConfiguration {
  @NonNull String host;
  @NonNull Integer port;
  @NonNull String user;
  @NonNull String password;
  @Nullable Map<String, String> jdbcProperties;

  @Override
  public String getJdbcUrl() {
    return "jdbc:mysql://" + host + ":" + port;
  }

  @Override
  public Properties getJdbcProperties() {
    var properties = new Properties();
    if (jdbcProperties != null) {
      jdbcProperties.forEach(properties::setProperty);
    }
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    // Create tables using a multi query.
    properties.setProperty("allowMultiQueries", "true");
    return properties;
  }
}
