package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import java.util.Set;

public interface ChangeEventProcessor {
  void process(Set<ChangeEvent> changeEvents);
}
