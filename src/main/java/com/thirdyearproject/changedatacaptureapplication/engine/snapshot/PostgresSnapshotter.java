package com.thirdyearproject.changedatacaptureapplication.engine.snapshot;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.thirdyearproject.changedatacaptureapplication.api.model.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.util.TypeConverter;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@Slf4j
public class PostgresSnapshotter extends Snapshotter {
  private static final String SET_TRANSACTION_ISOLATION_LEVEL =
      "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;";
  private static final String SET_TRANSACTION_SNAPSHOT = "SET TRANSACTION SNAPSHOT '%s'";

  private static final String CREATE_REPLICATION_SLOT =
      "CREATE_REPLICATION_SLOT \"cdc_replication_slot\" LOGICAL pgoutput";

  private static final String CREATE_PUBLICATION =
      "CREATE PUBLICATION cdc_publication FOR ALL TABLES;";

  private static final String SELECT_ALL = "SELECT * FROM %s";
  private static final Schema METADATA_SCHEMA =
      SchemaBuilder.struct()
          .field("walStartLsn", Schema.STRING_SCHEMA)
          .field("tableIdentifier", Schema.STRING_SCHEMA)
          .name("metadata")
          .build();
  private PostgresSnapshotInfo snapshotInfo;

  /* We need to use a separate connection for the replication slot as "snapshots are tied to the life cycle of their associated transaction."
  (Will Glynn, https://www.willglynn.com/2013/10/25/postgresql-snapshot-export/). We can then roll back once the snapshot is complete. */
  private JdbcConnection replicationSlotConnection;

  public PostgresSnapshotter(ConnectionConfiguration connectionConfig) {
    super(new JdbcConnection(connectionConfig));
    this.replicationSlotConnection = new JdbcConnection(connectionConfig);
  }

  @Override
  protected void createSnapshotEnvironment() throws SQLException {
    log.info("Creating Publication with Statement: {}", CREATE_PUBLICATION);
    var stmt = replicationSlotConnection.getConnection().createStatement();
    stmt.execute(CREATE_PUBLICATION);

    log.info("Creating snapshot replication slot with statement: {}", CREATE_REPLICATION_SLOT);
    this.replicationSlotConnection.setAutoCommit(true);
    stmt.execute(CREATE_REPLICATION_SLOT);
    var rs = stmt.getResultSet();
    if (rs.next()) {
      var walStartLsn = rs.getString("consistent_point");
      var snapshotName = rs.getString("snapshot_name");
      this.snapshotInfo =
          PostgresSnapshotInfo.builder()
              .walStartLsn(walStartLsn)
              .snapshotName(snapshotName)
              .build();
    }

    this.jdbcConnection.setAutoCommit(false);
    String setUpTransactionStmt =
        SET_TRANSACTION_ISOLATION_LEVEL
            + "\n"
            + String.format(SET_TRANSACTION_SNAPSHOT, snapshotInfo.getSnapshotName());
    log.info("Setting up snapshot transaction with statement: {}", setUpTransactionStmt);
    this.jdbcConnection.executeSqlWithoutCommitting(setUpTransactionStmt);
  }

  @Override
  protected void captureStructure(Set<String> tables) throws SQLException {
    try (var conn = jdbcConnection.getConnection()) {
      DatabaseMetaData metadata = conn.getMetaData();
      for (var tableStr : tables) {
        log.info(String.format("Capturing the structure of the following table: %s", tableStr));
        var schema = tableStr.split("\\.")[0];
        var table = tableStr.split("\\.")[1];

        var structSchemaBuilder = SchemaBuilder.struct().name(tableStr);
        try (var columnMetadata = metadata.getColumns(null, schema, table, null)) {
          while (columnMetadata.next()) {
            var columnName = columnMetadata.getString(4); // COLUMN_NAME
            var a = columnMetadata.getInt(5); // DATA_TYPE

            // TODO: Look into column length, should i be concerned if that data is lost.
            structSchemaBuilder.field(columnName, TypeConverter.sqlColumnToKafkaConnectType(a));
          }
          tableSchemaMap.put(tableStr, structSchemaBuilder.build());
        }
      }
    }
  }

  @Override
  protected void snapshotTables(Set<String> tables, ChangeEventProducer changeEventProducer)
      throws SQLException {
    try (var conn = jdbcConnection.getConnection();
        var stmt = conn.createStatement()) {
      for (var tableStr : tables) {
        var tableStruct = tableSchemaMap.get(tableStr);
        var tableColumnFields = tableStruct.fields();
        var rs = stmt.executeQuery(String.format(SELECT_ALL, tableStr));
        while (rs.next()) {
          var row = new Struct(tableStruct);
          for (var field : tableColumnFields) {
            var fieldName = field.name();
            var fieldType = field.schema().type();
            row.put(
                field,
                // TODO: Move this switch statement.
                switch (fieldType) {
                  case BOOLEAN -> rs.getBoolean(fieldName);
                  case INT8, INT16, INT32 -> rs.getInt(fieldName);
                  case INT64 -> rs.getLong(fieldName);
                  case FLOAT32, FLOAT64 -> rs.getFloat(fieldName);
                  default -> rs.getString(fieldName);
                });
          }
          var metadata = new Struct(METADATA_SCHEMA);
          metadata.put("walStartLsn", snapshotInfo.getWalStartLsn());
          metadata.put("tableIdentifier", tableStr);
          var changeEvent = ChangeEvent.builder().metadata(metadata).after(row).build();
          changeEventProducer.sendEvent(changeEvent);
        }
      }
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void snapshotComplete() throws SQLException {
    // log.info("Dropping replication slot with statement: {}", DROP_REPLICATION_SLOT);
    // jdbcConnection.executeSql(DROP_REPLICATION_SLOT);
  }
}
