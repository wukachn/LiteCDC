package com.thirdyearproject.changedatacaptureapplication.engine.streaming;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.ConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.PgOutputMessageDecoder;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresStreamer extends Streamer {
  private static final String CREATE_PUBLICATION =
      "CREATE PUBLICATION cdc_publication FOR ALL TABLES;";
  private static final String CREATE_REPLICATION_SLOT =
      "SELECT pg_create_logical_replication_slot('cdc_replication_slot', 'cdc_publication');";

  public PostgresStreamer(ConnectionConfiguration connectionConfiguration) {
    super(new JdbcConnection(connectionConfiguration));
  }

  @Override
  protected void initEnvironment() throws SQLException {}

  @Override
  protected void streamChanges() throws SQLException {
    try (var replicationStream = jdbcConnection.getReplicationStream()) {
      while (!replicationStream.isClosed()) {
        var message = replicationStream.read();
        if (message == null) {
          continue;
        }
        PgOutputMessageDecoder.processNotEmptyMessage(message);
      }
    } catch (Exception e) {
      e.printStackTrace();
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
