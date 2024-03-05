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
      // Can't universally use this property across connections as it causes syntax errors when
      // using batch requests with regular jdbc Statements.
      properties.setProperty("rewriteBatchedStatements", "true");
      super.connection = DriverManager.getConnection(connectionConfig.getJdbcUrl(), properties);
    }
    return this.connection;
  }
}
