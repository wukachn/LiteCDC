package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

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

  public List<ColumnDetails> getTableColumns(TableIdentifier tableId) throws SQLException {
    var columnList = new ArrayList<ColumnDetails>();

    var dbMetadata = this.getConnection().getMetaData();
    try (var columnMetadata =
            dbMetadata.getColumns(null, tableId.getSchema(), tableId.getTable(), null);
        var primaryKeyInfo =
            dbMetadata.getPrimaryKeys(null, tableId.getSchema(), tableId.getTable())) {

      Set<String> primaryKeyColumns = new HashSet<>();
      while (primaryKeyInfo.next()) {
        primaryKeyColumns.add(primaryKeyInfo.getString("COLUMN_NAME"));
      }

      while (columnMetadata.next()) {
        var columnName = columnMetadata.getString(4);
        var type = columnMetadata.getInt(5);
        var nullable = columnMetadata.getInt(11);
        var isNullable = nullable != ResultSetMetaData.columnNoNulls;
        var isPrimaryKey = primaryKeyColumns.contains(columnName);
        var size = columnMetadata.getInt("COLUMN_SIZE");
        var columnDetails =
            ColumnDetails.builder()
                .name(columnName)
                .sqlType(type)
                .isNullable(isNullable)
                .isPrimaryKey(isPrimaryKey)
                .size(size)
                .build();
        columnList.add(columnDetails);
      }
    }
    return columnList;
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
