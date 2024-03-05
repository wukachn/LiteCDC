package com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.CRUD;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.util.MySqlTypeUtils;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MySqlSink implements ChangeEventSink {
  protected static String DELETE_ROW = "DELETE FROM %s WHERE %s";
  protected static String UPSERT_ROW = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s";
  private static String ADD_COLUMN = "ALTER TABLE %s ADD COLUMN %s %s;";
  private static String DROP_COLUMN = "ALTER TABLE %s DROP COLUMN %s;";
  private static String ALTER_COLUMN = "ALTER TABLE %s MODIFY %s %s %s;";
  private static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (%s);";
  private static String CREATE_DB = "CREATE DATABASE IF NOT EXISTS %s;";
  protected JdbcConnection jdbcConnection;
  protected Map<TableIdentifier, List<ColumnDetails>> columnDetailsMap;

  public MySqlSink(JdbcConnection jdbcConnection) {
    this.jdbcConnection = jdbcConnection;
    this.columnDetailsMap = new HashMap<>();
  }

  @Override
  public void process(List<ChangeEvent> changeEvents) {
    Collections.sort(changeEvents);

    createDatabasesIfNotExists(changeEvents);
    createTablesIfNotExists(changeEvents);

    // Each row gets updated only when the new_offset > old_offset.
    deliverChanges(changeEvents);
  }

  protected abstract void deliverChanges(List<ChangeEvent> changeEvents);

  private void createDatabasesIfNotExists(List<ChangeEvent> changeEvents) {
    var createDatabasesSql = createDatabaseStatements(changeEvents);
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      jdbcConnection.setAutoCommit(true);
      stmt.execute(createDatabasesSql);
    } catch (SQLException e) {
      log.error("Could not ensure destination databases were created.", e);
    }
  }

  private void createTablesIfNotExists(List<ChangeEvent> changeEvents) {
    var createTablesSql = createTableStatements(changeEvents);
    if (createTablesSql.isEmpty()) {
      return;
    }
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      jdbcConnection.setAutoCommit(true);
      stmt.execute(createTablesSql);
    } catch (SQLException e) {
      log.error("Could not ensure destination tables were created.", e);
    }
  }

  private String createDatabaseStatements(List<ChangeEvent> changeEvents) {
    var databases =
        changeEvents.stream()
            .map(event -> "cdc_" + event.getMetadata().getTableId().getSchema())
            .collect(Collectors.toUnmodifiableSet());
    var sqlBuilder = new StringBuilder();
    for (var database : databases) {
      sqlBuilder.append(String.format(CREATE_DB, database));
    }
    return sqlBuilder.toString();
  }

  private String createTableStatements(List<ChangeEvent> changeEvents) {
    // Find first unique non-delete event.
    var uniqueTableChangeEvents =
        changeEvents.stream()
            .filter(changeEvent -> changeEvent.getMetadata().getOp() != CRUD.DELETE)
            .collect(
                Collectors.groupingBy(
                    event -> event.getMetadata().getTableId().getStringFormat(),
                    Collectors.minBy(
                        Comparator.comparing(
                            changeEvent -> changeEvent.getMetadata().getOffset()))))
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
    var columnCSV =
        String.format("%s,%s", String.join(",", columnValues), "cdc_last_updated varchar(60)");

    return String.format(CREATE_TABLE, table, columnCSV);
  }

  private String buildCreateTableColumnString(ColumnDetails details) {
    var columnName = details.getName();
    var columnType = MySqlTypeUtils.convertSqlTypeToString(details.getSqlType(), details.getSize());
    var primaryKey = details.isPrimaryKey() ? "PRIMARY KEY" : "";
    return String.format("%s %s %s", columnName, columnType, primaryKey);
  }

  // No support for column renaming.
  protected String compareStructureAndBuildSchemaChange(
      TableIdentifier tableIdentifier,
      List<ColumnDetails> beforeDetails,
      List<ColumnDetails> afterDetails)
      throws SQLException {
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

  protected String buildUpsertSqlString(ChangeEvent changeEvent, boolean isPreparedStatement) {
    var table = String.format("cdc_%s", changeEvent.getMetadata().getTableId().getStringFormat());

    var columnNames =
        changeEvent.getAfter().stream().map(column -> column.getDetails().getName()).toList();
    var namesCSV = String.format("%s,%s", String.join(",", columnNames), "cdc_last_updated");

    var valuesCSV = "";
    if (isPreparedStatement) {
      valuesCSV =
          IntStream.range(0, changeEvent.getAfter().size() + 1)
              .mapToObj(i -> "?")
              .collect(Collectors.joining(","));
    } else {
      var columnValues =
          changeEvent.getAfter().stream().map(column -> quoteIfString(column.getValue())).toList();
      valuesCSV =
          String.format(
              "%s,%s",
              String.join(",", columnValues), quoteIfString(changeEvent.getMetadata().getOffset()));
    }

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

    return String.format(UPSERT_ROW, table, namesCSV, valuesCSV, updateDetailsCSV);
  }

  protected String buildDeleteSql(ChangeEvent changeEvent) {
    var tableId = "cdc_" + changeEvent.getMetadata().getTableId().getStringFormat();

    var columnDetails = changeEvent.getBefore();
    var conditionBuilder = new StringBuilder();
    for (var column : columnDetails) {
      var details = column.getDetails();
      if (details.isPrimaryKey()) {
        conditionBuilder.append(
            String.format("%s = %s", details.getName(), quoteIfString(column.getValue())));
        conditionBuilder.append(" AND ");
      }
    }
    conditionBuilder.append(
        String.format(
            "cdc_last_updated < %s", quoteIfString(changeEvent.getMetadata().getOffset())));

    return String.format(DELETE_ROW, tableId, conditionBuilder);
  }

  protected String quoteIfString(Object value) {
    if (value != null && value.getClass().getName().equals("java.lang.String")) {
      return "\"" + value + "\"";
    }
    return String.valueOf(value);
  }
}
