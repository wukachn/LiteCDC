package com.thirdyearproject.changedatacaptureapplication.api.model.request;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.DestinationConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.SourceConfiguration;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

@Value
@Jacksonized
@Builder
@EqualsAndHashCode
public class PipelineConfiguration {
  @NonNull KafkaConfiguration kafkaConfig;
  @NonNull
  SourceConfiguration sourceConfig;
  @Nullable
  DestinationConfiguration destinationConfig;
}
