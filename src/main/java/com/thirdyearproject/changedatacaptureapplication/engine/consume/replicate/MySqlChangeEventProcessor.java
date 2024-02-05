package com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.mysql.MySqlConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.util.MySqlTypeUtils;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlChangeEventProcessor implements ChangeEventProcessor {
  private static String ADD_COLUMN = "ALTER TABLE %s ADD COLUMN %s %s;";
  private static String DROP_COLUMN = "ALTER TABLE %s DROP COLUMN %s;";
  private static String ALTER_COLUMN = "ALTER TABLE %s MODIFY %s %s %s;";
  private JdbcConnection jdbcConnection;
  private Map<TableIdentifier, List<ColumnDetails>> columnDetailsMap;

  public MySqlChangeEventProcessor(MySqlConnectionConfiguration connectionConfig) {
    this.jdbcConnection = new JdbcConnection(connectionConfig);
    this.columnDetailsMap = new HashMap<>();
  }

  @Override
  public void process(List<ChangeEvent> changeEvents) {
    if (changeEvents.isEmpty()) {
      return;
    }

    Collections.sort(changeEvents);

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
    var updateSql = buildUpdates(changeEvents);
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
                    Collectors.minBy(
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
    var columnType = MySqlTypeUtils.convertSqlTypeToString(details.getSqlType(), details.getSize());
    var primaryKey = details.isPrimaryKey() ? "PRIMARY KEY" : "";
    return String.format("%s %s %s", columnName, columnType, primaryKey);
  }

  private String buildUpdates(List<ChangeEvent> changeEvents) {
    var sqlBuilder = new StringBuilder();
    for (var changeEvent : changeEvents) {
      var tableId = changeEvent.getMetadata().getTableId();
      var afterDetails = changeEvent.getAfterColumnDetails();
      if (columnDetailsMap.containsKey(tableId)) {
        var beforeDetails = columnDetailsMap.get(tableId);
        var possibleAlterTableSql =
            compareAndBuildAlterTableSql(tableId, beforeDetails, afterDetails);
        sqlBuilder.append(possibleAlterTableSql);
      } else {
        columnDetailsMap.put(tableId, afterDetails);
      }
      sqlBuilder.append(buildSingleIdempotentUpdate(changeEvent));
    }
    return sqlBuilder.toString();
  }

  // No support for column renaming.
  private String compareAndBuildAlterTableSql(
      TableIdentifier tableIdentifier,
      List<ColumnDetails> beforeDetails,
      List<ColumnDetails> afterDetails) {
    var sqlBuilder = new StringBuilder();

    // Check for deleted columns.
    for (var beforeCol : beforeDetails) {
      var noLongerPresent =
          afterDetails.stream()
              .noneMatch(afterCol -> afterCol.getName().equals(beforeCol.getName()));
      if (noLongerPresent) {
        sqlBuilder.append(
            String.format(
                DROP_COLUMN,
                String.format("cdc_%s", tableIdentifier.getStringFormat()),
                beforeCol.getName()));
      }
    }

    // Check for new columns and changes.
    for (var afterCol : afterDetails) {
      var optionalBeforeCol =
          beforeDetails.stream()
              .filter(col -> col.getName().equals(afterCol.getName()))
              .findFirst();
      if (optionalBeforeCol.isEmpty()) {
        sqlBuilder.append(
            String.format(
                ADD_COLUMN,
                String.format("cdc_%s", tableIdentifier.getStringFormat()),
                afterCol.getName(),
                MySqlTypeUtils.convertSqlTypeToString(afterCol.getSqlType(), afterCol.getSize())));
        continue;
      }
      var beforeCol = optionalBeforeCol.get();
      if (beforeCol.getSqlType() != afterCol.getSqlType()
          || beforeCol.getSize() != afterCol.getSize()
          || beforeCol.isNullable() != afterCol.isNullable()) {
        sqlBuilder.append(
            String.format(
                ALTER_COLUMN,
                String.format("cdc_%s", tableIdentifier.getStringFormat()),
                afterCol.getName(),
                MySqlTypeUtils.convertSqlTypeToString(afterCol.getSqlType(), afterCol.getSize()),
                MySqlTypeUtils.convertNullableBooleanToString(afterCol.isNullable())));
      }
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
    if (value.getClass().getName().equals("java.lang.String")) {
      return "\"" + value + "\"";
    }
    return String.valueOf(value);
  }
}
