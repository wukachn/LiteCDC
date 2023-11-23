package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

@Slf4j
public class ChangeEventProducer {

  private static final AvroData CONVERTER =
      new AvroData(
          new AvroDataConfig.Builder()
              .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
              .build());
  private KafkaProducerService kafkaProducerService;

  public ChangeEventProducer(KafkaProducerService kafkaProducerService) {
    this.kafkaProducerService = kafkaProducerService;
  }

  public void sendEvent(ChangeEvent changeEvent) throws JsonMappingException {
    var tableIdString = changeEvent.getTableIdString();
    var genericRecord = convertToGenericRecord(changeEvent);

    kafkaProducerService.sendEvent(genericRecord, tableIdString);
  }

  private GenericRecord convertToGenericRecord(ChangeEvent changeEvent) {
    var connectSchema = changeEvent.getSchema();
    var avroSchema = CONVERTER.fromConnectSchema(connectSchema);

    var genericRecord = new GenericData.Record(avroSchema);

    var metadataValue =
        CONVERTER.fromConnectData(changeEvent.getMetadata().schema(), changeEvent.getMetadata());
    var beforeValue =
        CONVERTER.fromConnectData(changeEvent.getBefore().schema(), changeEvent.getBefore());
    var afterValue =
        CONVERTER.fromConnectData(changeEvent.getAfter().schema(), changeEvent.getAfter());

    genericRecord.put("metadata", metadataValue);
    genericRecord.put("before", beforeValue);
    genericRecord.put("after", afterValue);

    return genericRecord;
  }
}
