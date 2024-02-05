package com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.*;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.CRUD;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnWithData;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.PostgresMetadata;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.sql.*;
import java.util.ArrayList;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.replication.LogSequenceNumber;

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
      var walStartLsn = LogSequenceNumber.valueOf(rs.getString("consistent_point"));
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
  protected void captureStructure(Set<TableIdentifier> tables) throws SQLException {
    for (var table : tables) {
      log.info(
          String.format(
              "Capturing the structure of the following table: %s", table.getStringFormat()));

      var columns = jdbcConnection.getTableColumns(table);
      tableColumnMap.put(table, columns);
    }
  }

  @Override
  protected void snapshotTables(
      Set<TableIdentifier> tables, ChangeEventProducer changeEventProducer) throws SQLException {
    try (var conn = jdbcConnection.getConnection();
        var stmt = conn.createStatement()) {
      for (var table : tables) {
        var columnList = tableColumnMap.get(table);
        var rs = stmt.executeQuery(String.format(SELECT_ALL, table.getStringFormat()));
        while (rs.next()) {
          var after = new ArrayList<ColumnWithData>();
          for (var columnDetails : columnList) {
            var value = getFromResultSet(rs, columnDetails);
            after.add(ColumnWithData.builder().details(columnDetails).value(value).build());
          }
          var metadata =
              PostgresMetadata.builder()
                  .lsn(snapshotInfo.getWalStartLsn())
                  .op(CRUD.READ)
                  .tableId(table)
                  .build();
          var changeEvent =
              ChangeEvent.builder().metadata(metadata).before(null).after(after).build();

          changeEventProducer.sendEvent(changeEvent);
        }
      }
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void snapshotComplete() throws SQLException {}

  private Object getFromResultSet(ResultSet rs, ColumnDetails columnDetails) throws SQLException {
    switch (columnDetails.getSqlType()) {
      case Types.BOOLEAN -> {
        return rs.getBoolean(columnDetails.getName());
      }
      case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIT -> {
        return rs.getInt(columnDetails.getName());
      }
      case Types.BIGINT -> {
        return rs.getLong(columnDetails.getName());
      }
      case Types.FLOAT, Types.DOUBLE -> {
        return rs.getFloat(columnDetails.getName());
      }
      default -> {
        return rs.getString(columnDetails.getName());
      }
    }
  }
}
