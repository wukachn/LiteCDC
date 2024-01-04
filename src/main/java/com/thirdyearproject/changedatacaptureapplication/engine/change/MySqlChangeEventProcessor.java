package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.MySqlConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlChangeEventProcessor implements ChangeEventProcessor {
  private JdbcConnection jdbcConnection;

  public MySqlChangeEventProcessor(MySqlConnectionConfiguration connectionConfig) {
    this.jdbcConnection = new JdbcConnection(connectionConfig);
  }

  @Override
  public void process(List<ChangeEvent> changeEvents) {
    if (changeEvents.size() == 0) {
      return;
    }

    createDatabasesIfNotExists(changeEvents);
    createTablesIfNotExists(changeEvents);

    // Each row gets updated only when the new_offset > old_offset.
    deliverChanges(changeEvents);
  }

  private void createDatabasesIfNotExists(List<ChangeEvent> changeEvents) {
    var createDatabasesSql = createDatabaseStatements(changeEvents);
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      stmt.execute(createDatabasesSql);
    } catch (SQLException e) {
      log.error("Could not ensure destination databases were created.", e);
    }
  }

  private void createTablesIfNotExists(List<ChangeEvent> changeEvents) {
    var createTablesSql = createTableStatements(changeEvents);
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      stmt.execute(createTablesSql);
    } catch (SQLException e) {
      log.error("Could not ensure destination tables were created.", e);
    }
  }

  private void deliverChanges(List<ChangeEvent> changeEvents) {
    var updateSql = buildAllIdempotentUpdates(changeEvents);
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      stmt.execute(updateSql);
    } catch (SQLException e) {
      log.error("Could not deliver changes to destination database.", e);
    }
  }

  private String createDatabaseStatements(List<ChangeEvent> changeEvents) {
    var databases =
        changeEvents.stream()
            .map(event -> "cdc_" + event.getMetadata().getTableId().getSchema())
            .collect(Collectors.toUnmodifiableSet());
    var sqlBuilder = new StringBuilder();
    for (var database : databases) {
      sqlBuilder.append(String.format("CREATE DATABASE IF NOT EXISTS %s;", database));
    }
    return sqlBuilder.toString();
  }

  private String createTableStatements(List<ChangeEvent> changeEvents) {
    var uniqueTableChangeEvents =
        changeEvents.stream()
            .collect(
                Collectors.groupingBy(
                    event -> event.getMetadata().getTableId().getStringFormat(),
                    Collectors.maxBy(
                        Comparator.comparingLong(event -> event.getMetadata().getOffset()))))
            .values()
            .stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(
                Collectors.toMap(
                    event -> event.getMetadata().getTableId().getStringFormat(), event -> event))
            .values();

    var sqlBuilder = new StringBuilder();
    for (var uniqueTableChangeEvent : uniqueTableChangeEvents) {
      sqlBuilder.append(buildCreateTableStatement(uniqueTableChangeEvent));
    }
    return sqlBuilder.toString();
  }

  private String buildCreateTableStatement(ChangeEvent changeEvent) {
    var table = String.format("cdc_%s", changeEvent.getMetadata().getTableId().getStringFormat());

    var columnValues =
        changeEvent.getAfter().stream()
            .map(column -> buildCreateTableColumnString(column.getDetails()))
            .toList();
    var columnCSV = String.format("%s,%s", String.join(",", columnValues), "cdc_last_updated INT");

    return String.format("CREATE TABLE IF NOT EXISTS %s (%s);", table, columnCSV);
  }

  private String buildCreateTableColumnString(ColumnDetails details) {
    var columnName = details.getName();
    var columnType = convertSqlTypeToString(details.getSqlType(), details.getSize());
    var primaryKey = details.isPrimaryKey() ? "PRIMARY KEY" : "";
    return String.format("%s %s %s", columnName, columnType, primaryKey);
  }

  private String buildAllIdempotentUpdates(List<ChangeEvent> changeEvents) {
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
                        "%s = IF(VALUES(cdc_last_updated) > cdc_last_updated, VALUES(%s), %s)",
                        column.getDetails().getName(),
                        column.getDetails().getName(),
                        column.getDetails().getName()))
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

  private String convertSqlTypeToString(int type, int size) {
    switch (type) {
      case Types.BOOLEAN -> {
        return "BOOLEAN";
      }
      case Types.TINYINT -> {
        return String.format("TINYINT(%s)", size);
      }
      case Types.SMALLINT -> {
        return String.format("SMALLINT(%s)", size);
      }
      case Types.INTEGER -> {
        return String.format("INT(%s)", size);
      }
      case Types.BIT -> {
        return String.format("BIT(%s)", size);
      }
      case Types.BIGINT -> {
        return String.format("BIGINT(%s)", size);
      }
      case Types.FLOAT -> {
        return String.format("FLOAT(%s)", size);
      }
      case Types.DOUBLE -> {
        return String.format("DOUBLE(%s)", size);
      }
      default -> {
        return String.format("VARCHAR(%s)", size);
      }
    }
  }
}
