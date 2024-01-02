package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.api.model.database.MySqlConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlChangeEventProcessor implements ChangeEventProcessor {
  private MySqlConnectionConfiguration connectionConfig;

  public MySqlChangeEventProcessor(MySqlConnectionConfiguration connectionConfig) {
    this.connectionConfig = connectionConfig;
  }

  @Override
  public void process(ChangeEvent changeEvent) {
    log.info(String.valueOf(changeEvent));
  }
}
