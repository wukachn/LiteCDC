package com.thirdyearproject.changedatacaptureapplication.api;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ErrorMessage {
  String message;
}
