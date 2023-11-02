package com.thirdyearproject.changedatacaptureapplication.engine.change;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.apache.kafka.connect.data.Struct;

@Value
@Builder
@Jacksonized
public class ChangeEvent {
  Struct metadata;
  Struct after;
}
