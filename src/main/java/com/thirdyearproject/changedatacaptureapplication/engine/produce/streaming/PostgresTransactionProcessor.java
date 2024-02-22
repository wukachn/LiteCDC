package com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming;

import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.PostgresMetadata;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.replication.LogSequenceNumber;

@Slf4j
public class PostgresTransactionProcessor {
  private ChangeEventProducer changeEventProducer;
  private List<ChangeEvent> currentTransaction;

  PostgresTransactionProcessor(ChangeEventProducer changeEventProducer) {
    this.changeEventProducer = changeEventProducer;
    this.currentTransaction = new ArrayList<>();
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
  }
}
