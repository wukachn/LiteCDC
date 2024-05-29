package io.github.wukachn.litecdc.api.model.request.database;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.wukachn.litecdc.api.model.request.database.postgres.PostgresSourceConfiguration;
import io.github.wukachn.litecdc.engine.change.ChangeEventProducer;
import io.github.wukachn.litecdc.engine.change.model.TableIdentifier;
import io.github.wukachn.litecdc.engine.exception.SourceValidationException;
import io.github.wukachn.litecdc.engine.metrics.MetricsService;
import io.github.wukachn.litecdc.engine.produce.snapshot.Snapshotter;
import io.github.wukachn.litecdc.engine.produce.streaming.Streamer;
import java.sql.SQLException;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    value = {@JsonSubTypes.Type(value = PostgresSourceConfiguration.class, name = "postgres")})
public interface SourceConfiguration {
  Set<TableIdentifier> getTables();

  Snapshotter getSnapshotter(
      ChangeEventProducer changeEventProducer, MetricsService metricsService);

  Streamer getStreamer(ChangeEventProducer changeEventProducer, MetricsService metricsService);

  void validate() throws SQLException, SourceValidationException;
}
