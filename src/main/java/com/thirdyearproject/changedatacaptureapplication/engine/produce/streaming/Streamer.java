package com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import java.sql.SQLException;

public abstract class Streamer {
  JdbcConnection jdbcConnection;

  public Streamer(JdbcConnection jdbcConnection) {
    this.jdbcConnection = jdbcConnection;
  }

  public void stream(ChangeEventProducer changeEventProducer, MetricsService metricsService) {
    try {
      initEnvironment();
      streamChanges(changeEventProducer, metricsService);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void initEnvironment() throws SQLException;

  protected abstract void streamChanges(
      ChangeEventProducer changeEventProducer, MetricsService metricsService) throws SQLException;
}
