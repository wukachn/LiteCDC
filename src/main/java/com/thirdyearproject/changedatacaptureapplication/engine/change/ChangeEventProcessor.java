package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;

public interface ChangeEventProcessor {
  void process(ChangeEvent changeEvent);
}
