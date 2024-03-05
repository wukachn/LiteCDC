package com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.CRUD;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlTransactionalSink extends MySqlSink {

  private static final int BATCH_SIZE = 1000;

  public MySqlTransactionalSink(MySqlConnectionConfiguration connectionConfig) {
    super(new JdbcConnection(connectionConfig));
  }

  @Override
  protected void deliverChanges(List<ChangeEvent> changeEvents) {
    try (var conn = jdbcConnection.getConnection();
        var stmt = conn.createStatement()) {
      jdbcConnection.setAutoCommit(false);
      var currentBatch = 0;
      for (var changeEvent : changeEvents) {
        if (changeEvent.getMetadata().getOp() == CRUD.DELETE) {
          stmt.addBatch(buildDeleteSql(changeEvent));
        } else {
          var tableId = changeEvent.getMetadata().getTableId();
          var afterDetails = changeEvent.getAfterColumnDetails();
          if (columnDetailsMap.containsKey(tableId)) {
            var beforeDetails = columnDetailsMap.get(tableId);
            var possibleSchemaChangeSql =
                compareStructureAndBuildSchemaChange(tableId, beforeDetails, afterDetails);
            if (!possibleSchemaChangeSql.isEmpty()) {
              stmt.executeBatch();
              conn.commit();
              currentBatch = 0;

              try (var alterStmt = jdbcConnection.getConnection().createStatement()) {
                alterStmt.execute(possibleSchemaChangeSql);
              }
            }
          } else {
            columnDetailsMap.put(tableId, afterDetails);
          }
          stmt.addBatch(buildUpsertSqlString(changeEvent, false));
        }
        currentBatch += 1;
        if (currentBatch >= BATCH_SIZE) {
          stmt.executeBatch();
          conn.commit();
          currentBatch = 0;
        }
      }
      if (currentBatch > 0) {
        stmt.executeBatch();
        conn.commit();
      }
    } catch (SQLException e) {
      log.error("HELP!", e);
    }
  }
}
