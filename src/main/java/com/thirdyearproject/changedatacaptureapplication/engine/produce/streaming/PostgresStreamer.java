package com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.replication.PGReplicationStream;

@Slf4j
public class PostgresStreamer extends Streamer {

  private final PgOutputMessageDecoder pgOutputMessageDecoder;
  private final JdbcConnection replicationConnection;
  private PGReplicationStream replicationStream;

  public PostgresStreamer(ConnectionConfiguration connectionConfiguration) {
    super(new JdbcConnection(connectionConfiguration));
    this.replicationConnection = new JdbcConnection(connectionConfiguration);
    this.pgOutputMessageDecoder = new PgOutputMessageDecoder(jdbcConnection);
  }

  @Override
  protected void initEnvironment() throws SQLException {
    this.replicationStream = replicationConnection.getReplicationStream();
  }

  @Override
  protected void streamChanges(ChangeEventProducer changeEventProducer, MetricsService metricsService) {
    try {
      while (!replicationStream.isClosed() && !Thread.interrupted()) {
        var message = replicationStream.readPending();
        if (message == null) {
          continue;
        }
        var lsn = replicationStream.getLastReceiveLSN();
        var optionalEvent = pgOutputMessageDecoder.processNotEmptyMessage(message, lsn);
        if (optionalEvent.isPresent()) {
          changeEventProducer.sendEvent(optionalEvent.get());
        }
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }
}
