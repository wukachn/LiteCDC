package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

  @KafkaListener(topics = "thirdyearproject.public.newtable1", groupId = "testgroup")
  public void consume(ChangeEvent changeEvent) {
    log.info(String.valueOf(changeEvent));
  }
}
