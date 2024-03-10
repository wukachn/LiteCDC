package com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.ConnectionConfiguration;
import java.util.Properties;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Jacksonized
@Slf4j
public class MySqlConnectionConfiguration implements ConnectionConfiguration {

  @NonNull String host;
  @NonNull Integer port;
  @NonNull String user;
  @NonNull String password;

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
    properties.setProperty("allowPublicKeyRetrieval", "true");
    properties.setProperty("useSSL", "false");
    return properties;
  }
}
