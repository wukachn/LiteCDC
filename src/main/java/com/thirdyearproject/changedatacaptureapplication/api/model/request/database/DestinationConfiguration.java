package com.thirdyearproject.changedatacaptureapplication.api.model.request.database;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlDestinationConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate.ChangeEventSink;
import java.sql.SQLException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    value = {@JsonSubTypes.Type(value = MySqlDestinationConfiguration.class, name = "mysql")})
public interface DestinationConfiguration {
  ChangeEventSink createChangeEventSink();

  void validate() throws SQLException;
}
