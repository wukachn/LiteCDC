package com.thirdyearproject.changedatacaptureapplication.engine.snapshot;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Snapshotter {

  JdbcConnection jdbcConnection;
  Map<TableIdentifier, List<ColumnDetails>> tableColumnMap;

  public Snapshotter(JdbcConnection jdbcConnection) {
    this.jdbcConnection = jdbcConnection;
    this.tableColumnMap = new HashMap<>();
  }

  public void snapshot(Set<TableIdentifier> tables, ChangeEventProducer changeEventProducer) {
    log.info(
        String.format(
            "Starting snapshot on the following tables: %s",
            String.join(", ", tables.stream().map(TableIdentifier::getStringFormat).toList())));

    try {
      log.info("Step 1: Setting up start point of snapshot.");
      createSnapshotEnvironment();

      log.info("Step 2: Snapshotting structure of tables");
      captureStructure(tables);

      log.info("Step 3: Snapshotting content of tables");
      snapshotTables(tables, changeEventProducer);

      log.info("Snapshot Complete.");
      snapshotComplete();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void createSnapshotEnvironment() throws SQLException;

  protected abstract void captureStructure(Set<TableIdentifier> tables) throws SQLException;

  protected abstract void snapshotTables(
      Set<TableIdentifier> tables, ChangeEventProducer changeEventProducer) throws SQLException;

  protected abstract void snapshotComplete() throws SQLException;
}
