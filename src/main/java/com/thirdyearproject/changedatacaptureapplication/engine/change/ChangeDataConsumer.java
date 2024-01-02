package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ChangeDataConsumer implements Runnable {

  private KafkaConsumer<String, ChangeEvent> consumer;
  private String prefix;
  private TableIdentifier table;
  private ChangeEventProcessor eventProcessor;

  public ChangeDataConsumer(
      String bootstrapServer,
      String prefix,
      TableIdentifier table,
      ChangeEventProcessor eventProcessor) {
    this.consumer = createConsumer(bootstrapServer, table);
    this.prefix = prefix;
    this.table = table;
    this.eventProcessor = eventProcessor;
  }

  private KafkaConsumer<String, ChangeEvent> createConsumer(
      String bootstrapServer, TableIdentifier table) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.put(
        ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
        "cdc_" + table.getStringFormat() + UUID.randomUUID());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "cdc_" + table.getStringFormat());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ChangeEventDeserializer.class);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(properties);
  }

  @Override
  public void run() {
    consumer.subscribe(Collections.singletonList(prefix + "." + table.getStringFormat()));
    try {
      while (true) {
        ConsumerRecords<String, ChangeEvent> consumerRecords = consumer.poll(100);
        var changeEvents =
            StreamSupport.stream(consumerRecords.spliterator(), false)
                .map(ConsumerRecord::value)
                .collect(Collectors.toUnmodifiableSet());
        eventProcessor.process(changeEvents);
      }
    } catch (Exception e) {
      log.error("Consumer Error.", e);
    } finally {
      consumer.close();
    }
  }
}
