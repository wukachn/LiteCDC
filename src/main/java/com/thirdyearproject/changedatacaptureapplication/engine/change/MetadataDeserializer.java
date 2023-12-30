package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.Metadata;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.PostgresMetadata;
import java.lang.reflect.Type;

public class MetadataDeserializer implements JsonDeserializer<Metadata> {
  @Override
  public Metadata deserialize(
      JsonElement jsonElement, Type type, JsonDeserializationContext context)
      throws JsonParseException {
    JsonObject jsonObject = jsonElement.getAsJsonObject();

    if (jsonObject.has("lsn")) {
      return context.deserialize(jsonObject, PostgresMetadata.class);
    }

    return null;
  }
}
