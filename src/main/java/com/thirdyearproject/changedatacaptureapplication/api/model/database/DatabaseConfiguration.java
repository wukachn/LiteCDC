package com.thirdyearproject.changedatacaptureapplication.api.model.database;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.thirdyearproject.changedatacaptureapplication.engine.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.streaming.Streamer;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {@JsonSubTypes.Type(value = PostgresConfiguration.class, name = "postgres")})
public interface DatabaseConfiguration {
  Set<String> getTables();

  Snapshotter getSnapshotter();

  Streamer getStreamer();
}
