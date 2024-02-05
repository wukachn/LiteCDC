package com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import java.util.List;

public interface ChangeEventProcessor {
  void process(List<ChangeEvent> changeEvents);
}
