package com.thirdyearproject.changedatacaptureapplication.engine.snapshot;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Snapshotter {

  JdbcConnection jdbcConnection;

  public Snapshotter(JdbcConnection jdbcConnection) {
    this.jdbcConnection = jdbcConnection;
  }

  public void lockingSnapshot(Set<String> tables) {
    log.info(tables.toString());
  }
}
