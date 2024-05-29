package io.github.wukachn.litecdc.engine.produce.streaming;

import io.github.wukachn.litecdc.api.model.request.database.ConnectionConfiguration;
import io.github.wukachn.litecdc.engine.jdbc.JdbcConnection;
import io.github.wukachn.litecdc.engine.jdbc.PostgresReplicationConnection;
import io.github.wukachn.litecdc.engine.kafka.ChangeEventProducer;
import io.github.wukachn.litecdc.engine.metrics.MetricsService;
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
    this.pgOutputMessageDecoder =
        new PgOutputMessageDecoder(jdbcConnection, changeEventProducer, metricsService);
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
