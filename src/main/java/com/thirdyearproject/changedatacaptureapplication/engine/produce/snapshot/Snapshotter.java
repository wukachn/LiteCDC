package com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.PipelineException;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import java.io.IOException;
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
  ChangeEventProducer changeEventProducer;
  MetricsService metricsService;

  public Snapshotter(
      JdbcConnection jdbcConnection,
      ChangeEventProducer changeEventProducer,
      MetricsService metricsService) {
    this.jdbcConnection = jdbcConnection;
    this.tableColumnMap = new HashMap<>();
    this.changeEventProducer = changeEventProducer;
    this.metricsService = metricsService;
  }

  public void snapshot(Set<TableIdentifier> tables) throws PipelineException {
    log.info(
        String.format(
            "Starting snapshot on the following tables: %s",
            String.join(", ", tables.stream().map(TableIdentifier::getStringFormat).toList())));

    metricsService.startingSnapshot();
    try {
      log.info("Step 1: Setting up start point of snapshot.");
      createSnapshotEnvironment(tables);

      log.info("Step 2: Snapshotting structure of tables");
      captureStructure(tables);

      log.info("Step 3: Snapshotting content of tables");
      snapshotTables(tables);

      log.info("Snapshot Complete.");
      snapshotComplete();
    } catch (Exception e) {
      throw new PipelineException("Pipeline failed during snapshotting phase.", e);
    }
    metricsService.completingSnapshot();
  }

  protected abstract void createSnapshotEnvironment(Set<TableIdentifier> tables)
      throws SQLException;

  protected abstract void captureStructure(Set<TableIdentifier> tables) throws SQLException;

  protected abstract void snapshotTables(Set<TableIdentifier> tables) throws SQLException;

  protected abstract void snapshotComplete() throws SQLException, IOException;
}
