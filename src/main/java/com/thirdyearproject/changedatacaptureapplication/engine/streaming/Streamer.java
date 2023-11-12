package com.thirdyearproject.changedatacaptureapplication.engine.streaming;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import java.sql.SQLException;

public abstract class Streamer {
  JdbcConnection jdbcConnection;

  public Streamer(JdbcConnection jdbcConnection) {
    this.jdbcConnection = jdbcConnection;
  }

  public void stream() throws SQLException {
    initEnvironment();
    streamChanges();
  }

  protected abstract void initEnvironment() throws SQLException;

  protected abstract void streamChanges() throws SQLException;
}
