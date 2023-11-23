package com.thirdyearproject.changedatacaptureapplication.engine.change;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@Value
@Builder
@Jacksonized
public class ChangeEvent {
  Struct metadata;
  Struct before;
  Struct after;

  public Schema getSchema() {
    return SchemaBuilder.struct()
        .field("metadata", getMetadata().schema())
        .field("before", getBefore().schema())
        .field("after", getAfter().schema())
        .schema();
  }

  public String getTableIdString() {
    return (String) getMetadata().get("tableIdentifier");
  }
}
