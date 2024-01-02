package com.thirdyearproject.changedatacaptureapplication.api.model.database;

import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProcessor;
import com.thirdyearproject.changedatacaptureapplication.engine.change.MySqlChangeEventProcessor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Jacksonized
@Slf4j
public class MySqlDestinationConfiguration implements DestinationConfiguration {
  @NonNull MySqlConnectionConfiguration connectionConfig;

  @Override
  public ChangeEventProcessor createChangeEventProcessor() {
    return new MySqlChangeEventProcessor(connectionConfig);
  }
}
