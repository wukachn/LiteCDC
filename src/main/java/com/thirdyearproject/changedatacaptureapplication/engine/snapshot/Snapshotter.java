package com.thirdyearproject.changedatacaptureapplication.engine.snapshot;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;

@Slf4j
public abstract class Snapshotter {

  JdbcConnection jdbcConnection;
  Map<String, Schema> tableSchemaMap;

  public Snapshotter(JdbcConnection jdbcConnection) {
    this.jdbcConnection = jdbcConnection;
    this.tableSchemaMap = new HashMap<>();
  }

  public void snapshot(Set<String> tables, ChangeEventProducer changeEventProducer) {
    log.info(
        String.format("Starting snapshot on the following tables: %s", String.join(", ", tables)));

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

  protected abstract void captureStructure(Set<String> tables) throws SQLException;

  protected abstract void snapshotTables(
      Set<String> tables, ChangeEventProducer changeEventProducer) throws SQLException;

  protected abstract void snapshotComplete() throws SQLException;
}
