package com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlRegularSink extends MySqlSink {

  public MySqlRegularSink(MySqlConnectionConfiguration connectionConfig) {
    super(connectionConfig);
  }

  @Override
  protected void deliverChanges(List<ChangeEvent> changeEvents) {
    /*
    if (startTime == null) {
      startTime = Instant.now();
    }
    log.info(String.valueOf(Duration.between(startTime, Instant.now()).getSeconds()));

    Map<TableIdentifier, List<ChangeEvent>> groupedChanges =
        changeEvents.stream()
            .collect(Collectors.groupingBy(table -> table.getMetadata().getTableId()));

    for (var tableEntry : groupedChanges.entrySet()) {
      var tableId = tableEntry.getKey();
      var tableChanges = tableEntry.getValue();
      total += tableChanges.size();
      log.info(String.valueOf(total));
      var first = true;
      try (var conn = jdbcConnection.getConnection()) {
        conn.setAutoCommit(false);
        var loop = 1;
        var stmt = conn.prepareStatement(buildUpsertSqlString(changeEvents.get(0)));
        for (var changeEvent : tableChanges) {
          var afterDetails = changeEvent.getAfterColumnDetails();
          if (columnDetailsMap.containsKey(tableId)) {
            if (!first) {
              stmt.executeUpdate();
              stmt.close();
            }
            var beforeDetails = columnDetailsMap.get(tableId);
            var possibleAlterTableSql =
                compareAndBuildAlterTableSql(tableId, beforeDetails, afterDetails);
            if (!possibleAlterTableSql.isEmpty()) {
              try (var alter = conn.createStatement()) {
                alter.execute(possibleAlterTableSql);
              }
            }
          } else {
            columnDetailsMap.put(tableId, afterDetails);
          }
          if (stmt.isClosed()) {
            stmt = conn.prepareStatement(buildUpsertSqlString(changeEvents.get(0)));
          }
          var i = 1;
          for (var column : changeEvent.getAfter()) {
            // ignore deletes for now
            switch (column.getDetails().getSqlType()) {
              case Types.BOOLEAN -> stmt.setBoolean(i, (boolean) column.getValue());
              case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIT, Types.BIGINT -> stmt
                  .setLong(i, (long) column.getValue());
              case Types.FLOAT, Types.DOUBLE -> stmt.setDouble(i, (Double) column.getValue());
              default -> stmt.setString(i, (String) column.getValue());
            }
            i += 1;
          }
          stmt.addBatch();
          if (loop % 1000 == 0) {
            stmt.executeUpdate();
          }
          first = false;
          loop += 1;
        }
        stmt.executeUpdate();
        conn.commit();
      } catch (SQLException e) {
        log.error("HELP!", e);
      }
    }
     */
  }
}
