package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.temp.Column;
import com.thirdyearproject.changedatacaptureapplication.engine.temp.TableIdentifier;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.core.BaseConnection;
import org.postgresql.replication.PGReplicationStream;

// TODO: replace with ability to have multiple connections in parallel.
@Slf4j
public class JdbcConnection implements Closeable {
  private final ConnectionConfiguration connectionConfig;

  private Connection connection;

  public JdbcConnection(ConnectionConfiguration connectionConfig) {
    this.connectionConfig = connectionConfig;
  }

  public Connection getConnection() throws SQLException {
    if (this.connection == null || this.connection.isClosed()) {
      this.connection =
          DriverManager.getConnection(
              connectionConfig.getJdbcUrl(), connectionConfig.getBasicJdbcProperties());
    }
    return this.connection;
  }

  public PGReplicationStream getReplicationStream() throws SQLException {
    BaseConnection conn = (BaseConnection) getConnection();
    return conn.getReplicationAPI()
        .replicationStream()
        .logical()
        .withSlotName("cdc_replication_slot")
        .withSlotOption("proto_version", 1)
        .withSlotOption("publication_names", "cdc_publication")
        .start();
  }

  // Extra check in place to ensure correct auto commit
  // strategy.
  public void executeSqlWithoutCommitting(String sql) throws SQLException {
    if (connection.getAutoCommit()) {
      log.error("Auto commit is enabled, cannot execute without committing.");
      throw new RuntimeException("Auto commit is enabled, cannot execute without committing.");
    } else {
      try (var stmt = this.getConnection().createStatement()) {
        stmt.execute(sql);
      }
    }
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    this.getConnection().setAutoCommit(autoCommit);
  }

  public List<Column> getTableColumns(TableIdentifier tableId) throws SQLException {
    var columns = new ArrayList<Column>();
    var dbMetadata = this.getConnection().getMetaData();
    try (var columnMetadata =
        dbMetadata.getColumns(null, tableId.getSchema(), tableId.getTable(), null)) {
      while (columnMetadata.next()) {
        var columnName = columnMetadata.getString(4);
        var type = columnMetadata.getInt(5);
        var nullable = columnMetadata.getInt(11);
        var isNullable = nullable == ResultSetMetaData.columnNoNulls;
        var column = Column.builder().name(columnName).type(type).isNullable(isNullable).build();
        columns.add(column);
      }
    }
    return columns;
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
