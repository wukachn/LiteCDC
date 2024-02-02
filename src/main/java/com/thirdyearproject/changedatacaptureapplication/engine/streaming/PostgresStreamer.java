package com.thirdyearproject.changedatacaptureapplication.engine.streaming;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.PgOutputMessageDecoder;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.replication.PGReplicationStream;

@Slf4j
public class PostgresStreamer extends Streamer {
  private static final String CREATE_PUBLICATION =
      "CREATE PUBLICATION cdc_publication FOR ALL TABLES;";
  private static final String CREATE_REPLICATION_SLOT =
      "SELECT pg_create_logical_replication_slot('cdc_replication_slot', 'cdc_publication');";

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
  protected void streamChanges(ChangeEventProducer changeEventProducer) {
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

  private void createPublication() throws SQLException {
    log.info("Creating Publication with Statement: {}", CREATE_PUBLICATION);
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      stmt.execute(CREATE_PUBLICATION);
    }
  }

  private void createReplicationSlot() throws SQLException {
    log.info("Creating Replication Slot with Statement: {}", CREATE_REPLICATION_SLOT);
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      stmt.execute(CREATE_REPLICATION_SLOT);
    }
  }
}
