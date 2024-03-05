package com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.MySqlBatchingConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.CRUD;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlBatchingSink extends MySqlSink {

  private static final int BATCH_SIZE = 1000;

  public MySqlBatchingSink(MySqlConnectionConfiguration connectionConfig) {
    super(new MySqlBatchingConnection(connectionConfig));
    log.info("Consuming change events in BATCHING mode.");
  }

  @Override
  protected void deliverChanges(List<ChangeEvent> changeEvents) {
    Map<TableIdentifier, List<ChangeEvent>> groupedChanges =
        changeEvents.stream()
            .collect(Collectors.groupingBy(table -> table.getMetadata().getTableId()));

    for (var tableEntry : groupedChanges.entrySet()) {
      var tableId = tableEntry.getKey();
      var tableChangeBatches = buildBatches(tableEntry.getValue());

      for (var currentBatch : tableChangeBatches) {
        try {
          jdbcConnection.setAutoCommit(false);
          var firstEvent = currentBatch.get(0);
          if (firstEvent.getMetadata().getOp() == CRUD.DELETE) {
            handleDelete(firstEvent);
          } else {
            var afterDetails = firstEvent.getAfterColumnDetails();
            if (columnDetailsMap.containsKey(tableId)) {
              var beforeDetails = columnDetailsMap.get(tableId);
              handlePossibleSchemaChange(tableId, beforeDetails, afterDetails);
            } else {
              columnDetailsMap.put(tableId, afterDetails);
            }

            try (var stmt =
                jdbcConnection
                    .getConnection()
                    .prepareStatement(buildUpsertSqlString(firstEvent, true))) {
              for (var currentEvent : currentBatch) {
                setPreparedStatementValues(currentEvent, stmt);
                stmt.addBatch();
              }
              stmt.executeBatch();
            }
          }
          jdbcConnection.commit();
        } catch (SQLException e) {
          log.error("Failed to process batch.", e);
        }
      }
    }
  }

  private List<List<ChangeEvent>> buildBatches(List<ChangeEvent> changeEvents) {
    List<List<ChangeEvent>> batchList = new ArrayList<>();
    List<ChangeEvent> currentBatch = new ArrayList<>();
    for (var i = 0; i < changeEvents.size(); i++) {
      var currentEvent = changeEvents.get(i);
      currentBatch.add(currentEvent);
      if (currentEvent.getMetadata().getOp() == CRUD.DELETE || currentBatch.size() == BATCH_SIZE) {
        batchList.add(currentBatch);
        currentBatch = new ArrayList<>();
        continue;
      }

      if (i < changeEvents.size() - 1) {
        var currentEventColumnDetails = currentEvent.getAfterColumnDetails();
        var nextEventColumnDetails = changeEvents.get(i + 1).getAfterColumnDetails();
        if (currentEventColumnDetails.size() != nextEventColumnDetails.size()) {
          batchList.add(currentBatch);
          currentBatch = new ArrayList<>();
          continue;
        }

        var columnsEqual =
            currentEventColumnDetails.stream()
                    .allMatch(
                        currentEventColumn ->
                            nextEventColumnDetails.stream().anyMatch(currentEventColumn::equals))
                && nextEventColumnDetails.stream()
                    .allMatch(
                        nextEventColumn ->
                            currentEventColumnDetails.stream().anyMatch(nextEventColumn::equals));
        if (!columnsEqual) {
          batchList.add(currentBatch);
          currentBatch = new ArrayList<>();
        }
      }
    }
    if (!currentBatch.isEmpty()) {
      batchList.add(currentBatch);
    }
    return batchList;
  }

  private void handleDelete(ChangeEvent changeEvent) throws SQLException {
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      stmt.execute(buildDeleteSql(changeEvent));
    }
  }

  private void setPreparedStatementValues(ChangeEvent changeEvent, PreparedStatement stmt)
      throws SQLException {
    var columns = changeEvent.getAfter();
    if (columns != null) {
      var afterIter = columns.listIterator();
      while (afterIter.hasNext()) {
        var idx = afterIter.nextIndex() + 1;
        var column = afterIter.next();
        switch (column.getDetails().getSqlType()) {
          case Types.BOOLEAN -> stmt.setBoolean(idx, (boolean) column.getValue());
          case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIT, Types.BIGINT -> stmt
              .setLong(idx, (long) column.getValue());
          case Types.FLOAT, Types.DOUBLE -> stmt.setDouble(idx, (Double) column.getValue());
          default -> stmt.setString(idx, (String) column.getValue());
        }
      }
      stmt.setString(columns.size() + 1, changeEvent.getMetadata().getOffset());
    }
  }

  private void handlePossibleSchemaChange(
      TableIdentifier tableId, List<ColumnDetails> beforeDetails, List<ColumnDetails> afterDetails)
      throws SQLException {
    var possibleSchemaChangeSql =
        compareStructureAndBuildSchemaChange(tableId, beforeDetails, afterDetails);
    if (!possibleSchemaChangeSql.isEmpty()) {
      try (var stmt = jdbcConnection.getConnection().createStatement()) {
        stmt.execute(possibleSchemaChangeSql);
      }
      columnDetailsMap.put(tableId, afterDetails);
    }
  }
}
