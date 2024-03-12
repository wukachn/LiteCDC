package com.thirdyearproject.changedatacaptureapplication.engine.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.PipelineNotRunningException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class MetricsServiceTest {

  @Test
  public void metrics_not_running() {
    var metricsService = new MetricsService();
    assertThrows(PipelineNotRunningException.class, metricsService::getMetrics);
  }

  @Test
  public void metrics_initial() {
    var beforeEpoch = Instant.now().toEpochMilli();
    var metricsService = new MetricsService();
    var afterEpoch = Instant.now().toEpochMilli();
    metricsService.startingPipeline();
    metricsService.setPipelineStatus(PipelineStatus.SNAPSHOTTING);
    metricsService.initiateTables(
        Set.of(TableIdentifier.of("public", "table1"), TableIdentifier.of("public", "table2")));
    metricsService.startingSnapshot();

    var metrics = metricsService.getMetrics();

    assertTrue(
        beforeEpoch <= metrics.getPipelineStartTime()
            && metrics.getPipelineStartTime() <= afterEpoch);
    assertNull(metrics.getDbProducerTimeLagMs());
    assertNull(metrics.getProducerConsumerTimeLagMs());
    assertEquals(0, metrics.getNumOfTransactions());
    assertEquals(0, metrics.getTotalProduced());
    assertEquals(0, metrics.getTotalConsumed());
    for (var table : metrics.getTables()) {
      var ops = table.getOperationCounts();
      assertEquals(0, ops.getCreate());
      assertEquals(0, ops.getRead());
      assertEquals(0, ops.getUpdate());
      assertEquals(0, ops.getDelete());
    }
  }

  @Test
  public void snapshot_metrics_not_running() {
    var metricsService = new MetricsService();
    assertThrows(PipelineNotRunningException.class, metricsService::getSnapshotMetrics);
  }

  @Test
  public void snapshot_metrics_initial() {
    var metricsService = new MetricsService();
    metricsService.startingPipeline();
    metricsService.setPipelineStatus(PipelineStatus.SNAPSHOTTING);
    metricsService.initiateTables(
        Set.of(TableIdentifier.of("public", "table1"), TableIdentifier.of("public", "table2")));
    metricsService.startingSnapshot();

    var metrics = metricsService.getSnapshotMetrics();

    assertFalse(metrics.isCompleted());
    assertTrue(metrics.getDurationSeconds() >= 0);

    var expectedTables =
        List.of(
            TableRowsSnapshot.builder()
                .table(TableIdentifier.of("public", "table1"))
                .rows(0)
                .build(),
            TableRowsSnapshot.builder()
                .table(TableIdentifier.of("public", "table2"))
                .rows(0)
                .build());
    assertEquals(expectedTables, metrics.getTables());
  }
}
