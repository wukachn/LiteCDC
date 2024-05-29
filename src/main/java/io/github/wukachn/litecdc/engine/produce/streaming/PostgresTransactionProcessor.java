package io.github.wukachn.litecdc.engine.produce.streaming;

import io.github.wukachn.litecdc.engine.change.ChangeEventProducer;
import io.github.wukachn.litecdc.engine.change.model.ChangeEvent;
import io.github.wukachn.litecdc.engine.change.model.PostgresMetadata;
import io.github.wukachn.litecdc.engine.metrics.MetricsService;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.replication.LogSequenceNumber;

@Slf4j
public class PostgresTransactionProcessor {
  private ChangeEventProducer changeEventProducer;
  private List<ChangeEvent> currentTransaction;
  private MetricsService metricsService;

  PostgresTransactionProcessor(
      ChangeEventProducer changeEventProducer, MetricsService metricsService) {
    this.changeEventProducer = changeEventProducer;
    this.currentTransaction = new ArrayList<>();
    this.metricsService = metricsService;
  }

  public void process(ChangeEvent changeEvent) {
    currentTransaction.add(changeEvent);
  }

  public void commit(LogSequenceNumber commitLsn) {
    for (var changeEvent : currentTransaction) {
      var metadata = changeEvent.getMetadata();
      if (metadata instanceof PostgresMetadata) {
        ((PostgresMetadata) metadata).setCommitLsn(commitLsn);
        changeEventProducer.sendEvent(changeEvent);
      }
    }
    currentTransaction.clear();
    metricsService.incrementTxs();
  }
}
