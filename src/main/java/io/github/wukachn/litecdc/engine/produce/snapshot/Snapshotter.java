package io.github.wukachn.litecdc.engine.produce.snapshot;

import io.github.wukachn.litecdc.engine.change.model.ColumnDetails;
import io.github.wukachn.litecdc.engine.change.model.TableIdentifier;
import io.github.wukachn.litecdc.engine.exception.PipelineException;
import io.github.wukachn.litecdc.engine.jdbc.JdbcConnection;
import io.github.wukachn.litecdc.engine.kafka.ChangeEventProducer;
import io.github.wukachn.litecdc.engine.metrics.MetricsService;
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

  public void snapshot(Set<TableIdentifier> tables) throws PipelineException, IOException {
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
    } catch (Exception e) {
      throw new PipelineException("Pipeline failed during snapshotting phase.", e);
    } finally {
      snapshotComplete();
    }
    metricsService.completingSnapshot();
  }

  protected abstract void createSnapshotEnvironment(Set<TableIdentifier> tables)
      throws SQLException;

  protected abstract void captureStructure(Set<TableIdentifier> tables) throws SQLException;

  protected abstract void snapshotTables(Set<TableIdentifier> tables) throws SQLException;

  protected abstract void snapshotComplete() throws IOException;
}
