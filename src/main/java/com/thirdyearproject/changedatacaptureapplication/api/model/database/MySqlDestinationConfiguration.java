package com.thirdyearproject.changedatacaptureapplication.api.model.database;

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
}
