package com.thirdyearproject.changedatacaptureapplication.engine.kafka.serialization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.Metadata;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
public class ChangeEventDeserializer implements Deserializer<ChangeEvent> {

  private final Gson gson =
      new GsonBuilder()
          .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
          .registerTypeAdapter(Metadata.class, new MetadataDeserializer())
          .create();

  @Override
  public ChangeEvent deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    String jsonString = new String(data, StandardCharsets.UTF_8);
    return gson.fromJson(jsonString, ChangeEvent.class);
  }

  @Override
  public void close() {}
}
