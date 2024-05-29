package io.github.wukachn.litecdc.engine.change.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Value;

@Value(staticConstructor = "of")
public class TableIdentifier {
  String schema;
  String table;

  @JsonIgnore
  public String getStringFormat() {
    return String.format("%s.%s", schema, table);
  }
}
