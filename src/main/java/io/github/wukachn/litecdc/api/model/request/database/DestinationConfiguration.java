package io.github.wukachn.litecdc.api.model.request.database;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.wukachn.litecdc.api.model.request.database.mysql.MySqlDestinationConfiguration;
import io.github.wukachn.litecdc.engine.consume.ChangeEventSink;
import java.sql.SQLException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    value = {@JsonSubTypes.Type(value = MySqlDestinationConfiguration.class, name = "mysql")})
public interface DestinationConfiguration {
  ChangeEventSink createChangeEventSink();

  void validate() throws SQLException;
}
