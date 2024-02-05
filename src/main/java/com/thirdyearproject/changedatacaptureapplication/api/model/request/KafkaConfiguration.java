package com.thirdyearproject.changedatacaptureapplication.api.model.request;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@Builder
public class KafkaConfiguration {
  // TODO: Need to change remove these references in application.yml (how producer is created)
  @NonNull String topicPrefix;
  @NonNull String bootstrapServer;
}
