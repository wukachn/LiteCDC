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
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    properties.setProperty("allowMultiQueries", "true");

    // User defined jdbc properties can override any set by the application. The user should know
    // the risks.
    if (jdbcProperties != null) {
      jdbcProperties.forEach(properties::setProperty);
    }
    return properties;
  }
}
