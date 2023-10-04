package com.thirdyearproject.changedatacaptureapplication.api.model;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.DatabaseConfiguration;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.springframework.lang.NonNull;

@Value
@Jacksonized
@Builder
@EqualsAndHashCode
public class PipelineConfiguration {
  @NonNull DatabaseConfiguration databaseConfig;
}
