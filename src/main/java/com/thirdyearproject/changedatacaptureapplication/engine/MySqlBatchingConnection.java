package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.ConnectionConfiguration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySqlBatchingConnection extends JdbcConnection {
  public MySqlBatchingConnection(ConnectionConfiguration connectionConfig) {
    super(connectionConfig);
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (super.connection == null || super.connection.isClosed()) {
      var properties = connectionConfig.getBasicJdbcProperties();
      properties.setProperty("rewriteBatchedStatements", "true");
      super.connection = DriverManager.getConnection(connectionConfig.getJdbcUrl(), properties);
    }
    return this.connection;
  }
}
