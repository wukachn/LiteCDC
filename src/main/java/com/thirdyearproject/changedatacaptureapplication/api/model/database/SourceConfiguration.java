package com.thirdyearproject.changedatacaptureapplication.api.model.database;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.thirdyearproject.changedatacaptureapplication.api.model.database.postgres.PostgresSourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming.Streamer;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    value = {@JsonSubTypes.Type(value = PostgresSourceConfiguration.class, name = "postgres")})
public interface SourceConfiguration {
  Set<TableIdentifier> getTables();

  Snapshotter getSnapshotter();

  Streamer getStreamer();
}
