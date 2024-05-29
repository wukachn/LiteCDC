package io.github.wukachn.litecdc.engine.kafka.serialization;

import com.google.gson.Gson;
import io.github.wukachn.litecdc.engine.change.model.ChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class ChangeEventSerializer implements Serializer<ChangeEvent> {

  private final Gson gson = new Gson();

  @Override
  public byte[] serialize(String topic, ChangeEvent changeEvent) {
    return gson.toJson(changeEvent).getBytes();
  }

  @Override
  public void close() {}
}
