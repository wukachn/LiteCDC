package com.thirdyearproject.changedatacaptureapplication.api.model.database;

import java.util.Properties;
import lombok.NonNull;

public class MySqlConnectionConfiguration implements ConnectionConfiguration {

  @NonNull String host;
  @NonNull Integer port;
  @NonNull String database;
  @NonNull String user;
  @NonNull String password;

  @Override
  public String getJdbcUrl() {
    return "jdbc:mysql://" + host + ":" + port + "/" + database;
  }

  @Override
  public Properties getBasicJdbcProperties() {
    var properties = new Properties();
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    return properties;
  }
}
