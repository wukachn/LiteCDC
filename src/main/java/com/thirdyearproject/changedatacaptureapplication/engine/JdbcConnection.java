package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.ConnectionConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

// TODO: replace with ability to have multiple connections in parallel.
public class JdbcConnection implements Closeable {
  private final ConnectionConfiguration connectionConfig;

  private Connection connection;

  public JdbcConnection(ConnectionConfiguration connectionConfig) {
    this.connectionConfig = connectionConfig;
  }

  public Connection getConnection() throws SQLException {
    if (connection == null || connection.isClosed()) {
      connection = DriverManager.getConnection(connectionConfig.getJdbcUrl(), connectionConfig.getBasicJdbcProperties());
    }
    return connection;
  }

  @Override
  public void close() throws IOException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
