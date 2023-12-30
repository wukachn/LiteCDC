package com.thirdyearproject.changedatacaptureapplication.engine.change.model;

import lombok.Value;

@Value(staticConstructor = "of")
public class TableIdentifier {
  String schema;
  String table;

  public String getStringFormat() {
    return String.format("%s.%s", schema, table);
  }
}
