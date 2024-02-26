package com.thirdyearproject.changedatacaptureapplication.engine.metrics;

import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetMetricsResponse;
import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetPipelineStatusResponse;
import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetSnapshotMetricsResponse;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.CRUD;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.PipelineNotRunningException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Setter;
import org.javatuples.Pair;
import org.springframework.stereotype.Service;

@Service
public class MetricsService {

  @Setter PipelineStatus pipelineStatus = PipelineStatus.NOT_RUNNING;
  OffsetDateTime snapshotStartTime;
  OffsetDateTime snapshotEndTime;
  Map<TableIdentifier, Pair<Long, Boolean>> snapshotTracker = new HashMap<>();
  Map<TableIdentifier, CrudCount> crudTracker = new HashMap<>();
  OffsetDateTime pipelineStartTime;

  public GetPipelineStatusResponse getPipelineStatus() {
    return GetPipelineStatusResponse.builder().status(pipelineStatus).build();
  }

  public GetSnapshotMetricsResponse getSnapshotMetrics() {
    if (pipelineStatus == PipelineStatus.NOT_RUNNING) {
      throw new PipelineNotRunningException("Pipeline not running.");
    }
    return GetSnapshotMetricsResponse.builder()
        .completed(isSnapshotComplete())
        .durationSeconds(getSnapshotDurationSeconds())
        .tables(getRowsSnapshot())
        .build();
  }

  public GetMetricsResponse getMetrics() {
    if (pipelineStatus == PipelineStatus.NOT_RUNNING) {
      throw new PipelineNotRunningException("Pipeline not running.");
    }
    return GetMetricsResponse.builder()
        .pipelineStartTime(pipelineStartTime.toEpochSecond())
        .tables(getTableCrudCounts())
        .build();
  }

  public void updateSnapshotRows(TableIdentifier tableId, long rows, boolean completed) {
    snapshotTracker.put(tableId, Pair.with(rows, completed));
  }

  public void startingPipeline() {
    this.pipelineStartTime = OffsetDateTime.now(ZoneOffset.UTC);
  }

  public void startingSnapshot() {
    this.snapshotStartTime = OffsetDateTime.now(ZoneOffset.UTC);
  }

  public void completingSnapshot() {
    this.snapshotEndTime = OffsetDateTime.now(ZoneOffset.UTC);
  }

  public void clear() {
    this.pipelineStatus = PipelineStatus.NOT_RUNNING;
    this.snapshotStartTime = null;
    this.snapshotEndTime = null;
    this.snapshotTracker = new HashMap<>();
    this.pipelineStartTime = null;
    this.crudTracker = new HashMap<>();
  }

  private boolean isSnapshotComplete() {
    if (snapshotEndTime == null) {
      return false;
    }
    return true;
  }

  private long getSnapshotDurationSeconds() {
    if (snapshotStartTime == null) {
      return 0;
    }
    if (snapshotEndTime == null) {
      return ChronoUnit.SECONDS.between(snapshotStartTime, OffsetDateTime.now(ZoneOffset.UTC));
    }

    return ChronoUnit.SECONDS.between(snapshotStartTime, snapshotEndTime);
  }

  private List<TableRowsSnapshot> getRowsSnapshot() {
    List<TableRowsSnapshot> rowsSnapshot = new ArrayList<>();
    for (var entry : snapshotTracker.entrySet()) {
      var valuePair = entry.getValue();
      rowsSnapshot.add(
          TableRowsSnapshot.builder()
              .table(entry.getKey())
              .rows(valuePair.getValue0())
              .completed(valuePair.getValue1())
              .build());
    }
    return rowsSnapshot;
  }

  private List<TableCRUD> getTableCrudCounts() {
    List<TableCRUD> crudCounts = new ArrayList<>();
    for (var entry : crudTracker.entrySet()) {
      crudCounts.add(
          TableCRUD.builder().table(entry.getKey()).operationCounts(entry.getValue()).build());
    }
    return crudCounts;
  }

  public void trackEvent(TableIdentifier tableIdentifier, CRUD op) {
    var crudCount = crudTracker.get(tableIdentifier);
    if (crudCount == null) {
      crudCount = CrudCount.builder().build();
    }
    crudCount.incrementOperation(op);
    crudTracker.put(tableIdentifier, crudCount);
  }
}
