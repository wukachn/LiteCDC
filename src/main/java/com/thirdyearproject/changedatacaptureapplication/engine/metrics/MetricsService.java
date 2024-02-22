package com.thirdyearproject.changedatacaptureapplication.engine.metrics;

import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetPipelineStatusResponse;
import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetSnapshotMetricsResponse;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
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
  Map<TableIdentifier, Pair<Long, Boolean>> tableTracker = new HashMap<>();

  public GetPipelineStatusResponse getPipelineStatus() {
    return GetPipelineStatusResponse.builder().status(pipelineStatus).build();
  }

  public GetSnapshotMetricsResponse getSnapshotMetrics() {
    return GetSnapshotMetricsResponse.builder()
        .completed(isSnapshotComplete())
        .durationSeconds(getSnapshotDurationSeconds())
        .tables(getRowsSnapshot())
        .build();
  }

  public void updateSnapshotRows(TableIdentifier tableId, long rows, boolean completed) {
    tableTracker.put(tableId, Pair.with(rows, completed));
  }

  public void startingSnapshot() {
    this.snapshotStartTime = OffsetDateTime.now(ZoneOffset.UTC);
  }

  public void completingSnapshot() {
    this.snapshotEndTime = OffsetDateTime.now(ZoneOffset.UTC);
  }

  public void clear() {
    this.snapshotStartTime = null;
    this.snapshotEndTime = null;
    this.tableTracker = new HashMap<>();
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
    for (var entry : tableTracker.entrySet()) {
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
}
