package io.github.wukachn.litecdc.engine.consume.replicate;

import io.github.wukachn.litecdc.engine.change.model.ChangeEvent;
import java.util.List;

public interface ChangeEventSink {
  void process(List<ChangeEvent> changeEvents);
}
