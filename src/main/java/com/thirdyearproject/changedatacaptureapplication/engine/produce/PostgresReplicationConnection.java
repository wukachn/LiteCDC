package com.thirdyearproject.changedatacaptureapplication.engine.produce;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresReplicationConnection extends JdbcConnection {
  public PostgresReplicationConnection(ConnectionConfiguration connectionConfig) {
    super(connectionConfig);
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (super.connection == null || super.connection.isClosed()) {
      var properties = connectionConfig.getBasicJdbcProperties();
      properties.setProperty("replication", "database");
      properties.setProperty("preferQueryMode", "simple");
      super.connection = DriverManager.getConnection(connectionConfig.getJdbcUrl(), properties);
    }
    return this.connection;
  }
}
