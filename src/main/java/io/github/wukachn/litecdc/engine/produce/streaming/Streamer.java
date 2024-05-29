package io.github.wukachn.litecdc.engine.produce.streaming;

import io.github.wukachn.litecdc.engine.JdbcConnection;
import io.github.wukachn.litecdc.engine.exception.PipelineException;
import io.github.wukachn.litecdc.engine.metrics.MetricsService;
import java.io.IOException;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Streamer {
  JdbcConnection jdbcConnection;
  MetricsService metricsService;

  public Streamer(JdbcConnection jdbcConnection, MetricsService metricsService) {
    this.jdbcConnection = jdbcConnection;
    this.metricsService = metricsService;
  }

  public void stream() throws PipelineException {
    log.info("Starting to stream changes.");
    try {
      initEnvironment();
      streamChanges();
    } catch (Exception e) {
      throw new PipelineException("Pipeline failed during streaming phase.", e);
    }
  }

  protected abstract void initEnvironment() throws SQLException;

  protected abstract void streamChanges() throws SQLException, IOException;
}
