package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.MySqlConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import java.sql.SQLException;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlChangeEventProcessor implements ChangeEventProcessor {
  private JdbcConnection jdbcConnection;

  public MySqlChangeEventProcessor(MySqlConnectionConfiguration connectionConfig) {
    this.jdbcConnection = new JdbcConnection(connectionConfig);
  }

  @Override
  public void process(Set<ChangeEvent> changeEvents) {
    // Let's assume the destination table has been created already with the cdc_last_updated column.
    // For now, lets always override the destination record. Ignoring offset.
    if (changeEvents.size() == 0) {
      return;
    }

    var updateSql = buildAllIdempotentUpdates(changeEvents);
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      stmt.execute(updateSql);
    } catch (SQLException e) {
      log.error("Could not deliver changes to destination database.", e);
    }
  }

  private String buildAllIdempotentUpdates(Set<ChangeEvent> changeEvents) {
    var sqlBuilder = new StringBuilder();
    for (var changeEvent : changeEvents) {
      sqlBuilder.append(buildSingleIdempotentUpdate(changeEvent));
    }
    return sqlBuilder.toString();
  }

  private String buildSingleIdempotentUpdate(ChangeEvent changeEvent) {
    // Add a simple prefix TODO: Make this configurable.
    var table = String.format("cdc_%s", changeEvent.getMetadata().getTableId().getStringFormat());

    var columnNames =
        changeEvent.getAfter().stream().map(column -> column.getDetails().getName()).toList();
    var namesCSV = String.format("%s,%s", String.join(",", columnNames), "cdc_last_updated");

    var columnValues =
        changeEvent.getAfter().stream().map(column -> quoteIfString(column.getValue())).toList();
    var valuesCSV =
        String.format(
            "%s,%s", String.join(",", columnValues), changeEvent.getMetadata().getOffset());

    var updateDetails =
        changeEvent.getAfter().stream()
            .map(
                column ->
                    String.format(
                        "%s = VALUES(%s)",
                        column.getDetails().getName(), column.getDetails().getName()))
            .toList();
    var updateDetailsCSV =
        String.format(
            "%s,%s",
            String.join(",", updateDetails), "cdc_last_updated = VALUES(cdc_last_updated)");

    return String.format(
        "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s;",
        table, namesCSV, valuesCSV, updateDetailsCSV);
  }

  private String quoteIfString(Object value) {
    if (value.getClass().getName() == "java.lang.String") {
      return "\"" + value + "\"";
    }
    return String.valueOf(value);
  }
}
