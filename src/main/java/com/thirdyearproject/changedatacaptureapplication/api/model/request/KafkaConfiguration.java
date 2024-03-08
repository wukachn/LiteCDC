package com.thirdyearproject.changedatacaptureapplication.api.model.request;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@Builder
public class KafkaConfiguration {
  @NonNull String topicPrefix;
  @NonNull String bootstrapServer;
  @NonNull TopicStrategy topicStrategy;
}
