package com.thirdyearproject.changedatacaptureapplication.api.model.request.database;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresSourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming.Streamer;
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

  void validate() throws SQLException;
}
