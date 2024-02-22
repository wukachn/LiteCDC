package com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import java.sql.SQLException;

public abstract class Streamer {
  JdbcConnection jdbcConnection;
  MetricsService metricsService;

  public Streamer(JdbcConnection jdbcConnection, MetricsService metricsService) {
    this.jdbcConnection = jdbcConnection;
    this.metricsService = metricsService;
  }

  public void stream() {
    try {
      initEnvironment();
      streamChanges();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void initEnvironment() throws SQLException;

  protected abstract void streamChanges() throws SQLException;
}
