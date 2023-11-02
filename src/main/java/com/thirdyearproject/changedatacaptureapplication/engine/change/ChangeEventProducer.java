package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaBuilder;

@Slf4j
public class ChangeEventProducer {

  private KafkaProducerService kafkaProducerService;

  public ChangeEventProducer(KafkaProducerService kafkaProducerService) {
    this.kafkaProducerService = kafkaProducerService;
  }

  public void sendEvent(ChangeEvent changeEvent) throws JsonMappingException {
    // TODO: Clean Up
    AvroData converter =
        new AvroData(
            new AvroDataConfig.Builder()
                // Don't add converter metadata fields to the output.
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .build());

    var tableIdentifier = (String) changeEvent.getMetadata().get("tableIdentifier");
    var changeSchema =
        SchemaBuilder.struct()
            .field("metadata", changeEvent.getMetadata().schema())
            .field("after", changeEvent.getAfter().schema())
            .schema();
    var schema = converter.fromConnectSchema(changeSchema);

    GenericRecord avroRecord = new GenericData.Record(schema);
    var metadataValue =
        converter.fromConnectData(changeEvent.getMetadata().schema(), changeEvent.getMetadata());
    var afterValue =
        converter.fromConnectData(changeEvent.getAfter().schema(), changeEvent.getAfter());
    avroRecord.put("metadata", metadataValue);
    avroRecord.put("after", afterValue);

    kafkaProducerService.sendEvent(avroRecord, tableIdentifier);
  }
}
