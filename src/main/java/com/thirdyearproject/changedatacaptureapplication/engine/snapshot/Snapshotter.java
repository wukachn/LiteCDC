package com.thirdyearproject.changedatacaptureapplication.engine.snapshot;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import java.sql.SQLException;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Snapshotter {

  JdbcConnection jdbcConnection;

  public Snapshotter(JdbcConnection jdbcConnection) {
    this.jdbcConnection = jdbcConnection;
  }

  public void snapshot(Set<String> tables) {
    log.info("Starting Snapshot");
    log.info(
        String.format(
            "Attempting to snapshot the following tables: %s", String.join(", ", tables)));

    try {
      log.info("Setting up start point of snapshot.");
      createSnapshotEnvironment();

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void createSnapshotEnvironment() throws SQLException;

  protected abstract void snapshotComplete() throws SQLException;
}
