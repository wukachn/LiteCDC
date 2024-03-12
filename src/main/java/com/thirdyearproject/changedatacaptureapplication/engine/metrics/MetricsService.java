package com.thirdyearproject.changedatacaptureapplication.engine.metrics;

import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetMetricsResponse;
import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetPipelineStatusResponse;
import com.thirdyearproject.changedatacaptureapplication.api.model.response.GetSnapshotMetricsResponse;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.PipelineNotRunningException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Setter;
import org.javatuples.Pair;
import org.springframework.stereotype.Service;

@Service
public class MetricsService {

  private @Setter PipelineStatus pipelineStatus = PipelineStatus.NOT_RUNNING;
  private Instant snapshotStartTime;
  private Instant snapshotEndTime;
  private Map<TableIdentifier, Pair<Long, Boolean>> snapshotTracker = new HashMap<>();
  private Map<TableIdentifier, CrudCount> crudTracker = new HashMap<>();
  private Instant pipelineStartTime;
  private Long dbProducerTimeLagMs;
  private Long producerConsumerTimeLagMs;
  private long numOfTransactions;
  private long totalConsumed;
  private long totalProduced;

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
        .pipelineStartTime(pipelineStartTime.toEpochMilli())
        .dbProducerTimeLagMs(dbProducerTimeLagMs)
        .producerConsumerTimeLagMs(producerConsumerTimeLagMs)
        .numOfTransactions(numOfTransactions)
        .totalProduced(totalProduced)
        .totalConsumed(totalConsumed)
        .tables(getTableCrudCounts())
        .build();
  }

  public void updateSnapshotRows(TableIdentifier tableId, long rows, boolean completed) {
    snapshotTracker.put(tableId, Pair.with(rows, completed));
  }

  public void startingPipeline() {
    this.pipelineStartTime = Instant.now();
  }

  public void startingSnapshot() {
    this.snapshotStartTime = Instant.now();
  }

  public void completingSnapshot() {
    this.snapshotEndTime = Instant.now();
  }

  public void clear() {
    this.pipelineStatus = PipelineStatus.NOT_RUNNING;
    this.snapshotStartTime = null;
    this.snapshotEndTime = null;
    this.snapshotTracker = new HashMap<>();
    this.pipelineStartTime = null;
    this.crudTracker = new HashMap<>();
    this.dbProducerTimeLagMs = null;
    this.producerConsumerTimeLagMs = null;
    this.numOfTransactions = 0;
    this.totalProduced = 0;
    this.totalConsumed = 0;
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
      return ChronoUnit.SECONDS.between(snapshotStartTime, Instant.now());
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

  public void produceEvent(ChangeEvent changeEvent) {
    var metadata = changeEvent.getMetadata();

    var tableIdentifier = metadata.getTableId();
    var crudCount = crudTracker.get(tableIdentifier);
    if (crudCount == null) {
      crudCount = CrudCount.builder().build();
    }
    crudCount.incrementOperation(metadata.getOp());
    crudTracker.put(tableIdentifier, crudCount);

    var commitTime = metadata.getDbCommitTime();
    if (commitTime != null) {
      this.dbProducerTimeLagMs = metadata.getProducedTime() - metadata.getDbCommitTime();
    }
    this.totalProduced += 1;
  }

  public void consumeEvents(List<ChangeEvent> changeEvents) {
    var producedTime = changeEvents.get(changeEvents.size() - 1).getMetadata().getProducedTime();
    this.producerConsumerTimeLagMs = Instant.now().toEpochMilli() - producedTime;
    this.totalConsumed += changeEvents.size();
  }

  public void initiateTables(Set<TableIdentifier> tables) {
    for (var table : tables) {
      snapshotTracker.put(table, Pair.with(0L, false));
      crudTracker.put(table, CrudCount.builder().build());
    }
  }

  public void incrementTxs() {
    this.numOfTransactions += 1;
  }
}
