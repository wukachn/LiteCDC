package com.thirdyearproject.changedatacaptureapplication.api.model.database;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProcessor;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(
    value = {@JsonSubTypes.Type(value = MySqlDestinationConfiguration.class, name = "mysql")})
public interface DestinationConfiguration {
  ChangeEventProcessor createChangeEventProcessor();
}
