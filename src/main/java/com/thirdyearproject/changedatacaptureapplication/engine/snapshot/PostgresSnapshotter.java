package com.thirdyearproject.changedatacaptureapplication.engine.snapshot;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresSnapshotter extends Snapshotter {
  private static final String SET_TRANSACTION_ISOLATION_LEVEL =
      "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;";
  private static final String SET_TRANSACTION_SNAPSHOT = "SET TRANSACTION SNAPSHOT '%s'";

  private static final String CREATE_REPLICATION_SLOT =
      "CREATE_REPLICATION_SLOT \"cdc_snapshot\" LOGICAL pgoutput";

  private static final String DROP_REPLICATION_SLOT =
      "SELECT PG_DROP_REPLICATION_SLOT('cdc_snapshot')";
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
    log.info("Creating snapshot replication slot with statement: {}", CREATE_REPLICATION_SLOT);
    this.replicationSlotConnection.setAutoCommit(true);
    var stmt = this.replicationSlotConnection.getConnection().createStatement();
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
    log.info("Setting up snapshot transaction with statement: {}", DROP_REPLICATION_SLOT);
    this.jdbcConnection.executeSqlWithoutCommitting(setUpTransactionStmt);
  }

  @Override
  protected void snapshotComplete() throws SQLException {
    log.info("Dropping replication slot with statement: {}", DROP_REPLICATION_SLOT);
    jdbcConnection.executeSql(DROP_REPLICATION_SLOT);
  }
}
