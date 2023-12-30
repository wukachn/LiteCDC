package com.thirdyearproject.changedatacaptureapplication.engine.change.model;

import java.util.List;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

@Value
@Builder
@Jacksonized
public class ChangeEvent {
  @NonNull Metadata metadata;
  @Nullable List<ColumnWithData> before;
  @Nullable List<ColumnWithData> after;
}
