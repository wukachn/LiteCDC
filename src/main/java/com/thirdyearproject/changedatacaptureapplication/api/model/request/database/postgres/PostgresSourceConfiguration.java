package com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.SourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.SourceValidationException;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot.PostgresSnapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming.PostgresStreamer;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming.Streamer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;

@Value
@Builder
@Jacksonized
@Slf4j
public class PostgresSourceConfiguration implements SourceConfiguration {
  @NonNull PostgresConnectionConfiguration connectionConfig;
  @NonNull Set<TableIdentifier> capturedTables;
  @Nullable String replicationSlot;
  @Nullable String publication;

  private String getReplicationSlot() {
    if (replicationSlot != null) {
      return replicationSlot;
    }
    return "cdc_replication_slot";
  }

  private String getPublication() {
    if (publication != null) {
      return publication;
    }
    return "cdc_publication";
  }

  @Override
  public Set<TableIdentifier> getTables() {
    return capturedTables;
  }

  @Override
  public Snapshotter getSnapshotter(
      ChangeEventProducer changeEventProducer, MetricsService metricsService) {
    return new PostgresSnapshotter(
        connectionConfig,
        changeEventProducer,
        metricsService,
        getPublication(),
        getReplicationSlot());
  }

  @Override
  public Streamer getStreamer(
      ChangeEventProducer changeEventProducer, MetricsService metricsService) {
    return new PostgresStreamer(
        connectionConfig,
        changeEventProducer,
        metricsService,
        getPublication(),
        getReplicationSlot());
  }

  @Override
  public void validate() throws SQLException {
    var jdbcConnection = new JdbcConnection(connectionConfig);
    validateWalLevel(jdbcConnection);
    validatePublication(jdbcConnection);
    validateReplicationSlot(jdbcConnection);
    validateSelectPermissions(jdbcConnection);
  }

  private void validateWalLevel(JdbcConnection jdbcConnection) throws SQLException {
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      var rs = stmt.executeQuery("SHOW wal_level");
      if (rs.next()) {
        if (!rs.getString("wal_level").equals("logical")) {
          throw new SourceValidationException("Invalid 'wal_level'. Must use logical.");
        }
      } else {
        log.warn("Failed to ensure that the correct 'wal_level' is being used.");
      }
    }
  }

  private void validatePublication(JdbcConnection jdbcConnection) throws SQLException {
    var publication = getPublication();
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      var rs =
          stmt.executeQuery(
              String.format("SELECT * FROM pg_publication WHERE pubname = '%s'", publication));
      if (rs.next()) {
        throw new SourceValidationException(
            String.format("Publication already exists: %s", publication));
      }
    }
  }

  private void validateReplicationSlot(JdbcConnection jdbcConnection) throws SQLException {
    var slot = getReplicationSlot();
    try (var stmt = jdbcConnection.getConnection().createStatement()) {
      var rs =
          stmt.executeQuery(
              String.format("SELECT * FROM pg_replication_slots WHERE slot_name = '%s'", slot));
      if (rs.next()) {
        throw new SourceValidationException(
            String.format("Replication slot already exists: %s", slot));
      }
    }
  }

  private void validateSelectPermissions(JdbcConnection jdbcConnection) throws SQLException {
    var missingPermsTables = new ArrayList<String>();
    for (var table : capturedTables) {
      try (var stmt = jdbcConnection.getConnection().createStatement()) {
        var rs =
            stmt.executeQuery(
                String.format(
                    "SELECT exists(SELECT * FROM information_schema.table_privileges where table_schema='%s' and table_name='%s' and privilege_type='SELECT')",
                    table.getSchema(), table.getTable()));
        if (rs.next()) {
          if (!rs.getBoolean("exists")) {
            missingPermsTables.add(table.getStringFormat());
          }
        } else {
          log.warn(String.format("Failed to validate SELECT permissions for table: ", table.getStringFormat()));
        }
      }
    }
    if (!missingPermsTables.isEmpty()) {
      throw new SourceValidationException(
          String.format(
              "SELECT permissions missing for tables: %s",
              missingPermsTables.stream().collect(Collectors.joining(", "))));
    }
  }
}
