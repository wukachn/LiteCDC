package com.thirdyearproject.changedatacaptureapplication.engine.snapshot;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;

public class PostgresSnapshotter extends Snapshotter {

  public PostgresSnapshotter(JdbcConnection jdbcConnection) {
    super(jdbcConnection);
  }
}
