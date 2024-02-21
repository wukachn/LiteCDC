package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate.ChangeEventProcessor;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.serialization.ChangeEventDeserializer;
import java.util.List;
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
  private String topicPrefix;
  private List<TableIdentifier> tables;
  private ChangeEventProcessor eventProcessor;

  public ChangeDataConsumer(
      String bootstrapServer,
      String topicPrefix,
      List<TableIdentifier> tables,
      ChangeEventProcessor eventProcessor) {
    this.consumer = createConsumer(bootstrapServer, tables);
    this.topicPrefix = topicPrefix;
    this.tables = tables;
    this.eventProcessor = eventProcessor;
  }

  private KafkaConsumer<String, ChangeEvent> createConsumer(
      String bootstrapServer, List<TableIdentifier> tables) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.put(
        ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
        "cdc_" + tables.get(0).getSchema() + UUID.randomUUID());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "cdc_" + tables.get(0).getSchema());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ChangeEventDeserializer.class);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(properties);
  }

  @Override
  public void run() {
    var topics = tables.stream().map(table -> topicPrefix + "." + table.getStringFormat()).toList();
    consumer.subscribe(topics);
    try {
      while (true) {
        ConsumerRecords<String, ChangeEvent> consumerRecords = consumer.poll(1000);
        List<ChangeEvent> changeEvents =
            StreamSupport.stream(consumerRecords.spliterator(), false)
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());
        eventProcessor.process(changeEvents);
        consumer.commitSync(); // TODO: do i need this?
      }
    } catch (Exception e) {
      log.error("Consumer Error.", e);
    } finally {
      consumer.close();
    }
  }
}
