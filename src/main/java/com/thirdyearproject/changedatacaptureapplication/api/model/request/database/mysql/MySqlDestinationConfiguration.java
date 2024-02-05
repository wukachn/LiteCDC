package com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.DestinationConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate.ChangeEventProcessor;
import com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate.MySqlChangeEventProcessor;
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
