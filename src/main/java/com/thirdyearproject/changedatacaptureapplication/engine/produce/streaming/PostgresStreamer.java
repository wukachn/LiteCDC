package com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.PostgresReplicationConnection;
import java.io.IOException;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.core.BaseConnection;
import org.postgresql.replication.PGReplicationStream;

@Slf4j
public class PostgresStreamer extends Streamer {

  private final PgOutputMessageDecoder pgOutputMessageDecoder;
  private final JdbcConnection replicationConnection;
  private PGReplicationStream replicationStream;
  private String replicationSlot;
  private String publication;

  public PostgresStreamer(
      ConnectionConfiguration connectionConfiguration,
      ChangeEventProducer changeEventProducer,
      MetricsService metricsService,
      String publication,
      String replicationSlot) {
    super(new JdbcConnection(connectionConfiguration), metricsService);
    this.replicationConnection = new PostgresReplicationConnection(connectionConfiguration);
    this.pgOutputMessageDecoder = new PgOutputMessageDecoder(jdbcConnection, changeEventProducer);
    this.publication = publication;
    this.replicationSlot = replicationSlot;
  }

  @Override
  protected void initEnvironment() throws SQLException {
    BaseConnection conn = (BaseConnection) replicationConnection.getConnection();
    this.replicationStream =
        conn.getReplicationAPI()
            .replicationStream()
            .logical()
            .withSlotName(replicationSlot)
            .withSlotOption("proto_version", 1)
            .withSlotOption("publication_names", publication)
            .start();
  }

  @Override
  protected void streamChanges() throws SQLException, IOException {
    try {
      while (!replicationStream.isClosed()) {
        var message = replicationStream.readPending();
        if (message == null) {
          continue;
        }
        var lsn = replicationStream.getLastReceiveLSN();
        pgOutputMessageDecoder.processNotEmptyMessage(message, lsn);
      }
    } finally {
      replicationStream.close();
      replicationConnection.close();
    }
  }
}
