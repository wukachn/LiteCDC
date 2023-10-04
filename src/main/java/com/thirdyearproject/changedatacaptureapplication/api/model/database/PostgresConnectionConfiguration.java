package com.thirdyearproject.changedatacaptureapplication.api.model.database;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Properties;
import lombok.NonNull;

public class PostgresConnectionConfiguration implements ConnectionConfiguration {
  @NonNull String host;
  @NonNull Integer port;
  @NonNull String database;
  @NonNull String user;
  @NonNull String password;

  @Override
  @JsonIgnore
  public String getJdbcUrl() {
    return "jdbc:postgresql://" + host + ":" + port + "/" + database;
  }

  @Override
  @JsonIgnore
  public Properties getBasicJdbcProperties() {
    var properties = new Properties();
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    return properties;
  }
}
