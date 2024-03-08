package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.TopicStrategy;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate.ChangeEventSink;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.serialization.ChangeEventDeserializer;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
  private ChangeEventSink eventProcessor;
  private MetricsService metricsService;
  private TopicStrategy topicStrategy;

  public ChangeDataConsumer(
      String bootstrapServer,
      String topicPrefix,
      List<TableIdentifier> tables,
      ChangeEventSink eventProcessor,
      MetricsService metricsService,
      TopicStrategy topicStrategy) {
    this.tables = tables;
    this.topicStrategy = topicStrategy;
    this.consumer = createConsumer(bootstrapServer);
    this.topicPrefix = topicPrefix;
    this.eventProcessor = eventProcessor;
    this.metricsService = metricsService;
  }

  private KafkaConsumer<String, ChangeEvent> createConsumer(String bootstrapServer) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    String groupInstanceId;
    String groupId;
    if (topicStrategy == TopicStrategy.PER_TABLE) {
      groupInstanceId = "cdc_" + tables.get(0).getSchema() + UUID.randomUUID();
      groupId = "cdc_" + tables.get(0).getSchema();
    } else {
      groupInstanceId = "cdc_all_tables" + UUID.randomUUID();
      groupId = "cdc_all_tables";
    }
    properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ChangeEventDeserializer.class);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 419430400);
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
    properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 419430400);
    return new KafkaConsumer<>(properties);
  }

  @Override
  public void run() {
    List<String> topics;
    if (topicStrategy == TopicStrategy.PER_TABLE) {
      topics = tables.stream().map(table -> topicPrefix + "." + table.getStringFormat()).toList();
    } else {
      topics = List.of(topicPrefix + ".all_tables");
    }

    consumer.subscribe(topics);
    try {
      while (true) {
        ConsumerRecords<String, ChangeEvent> consumerRecords =
            consumer.poll(Duration.of(2000, ChronoUnit.MILLIS));
        List<ChangeEvent> changeEvents =
            StreamSupport.stream(consumerRecords.spliterator(), true)
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());
        if (!changeEvents.isEmpty()) {
          metricsService.consumeEvent(changeEvents.get(changeEvents.size() - 1));
          eventProcessor.process(changeEvents);
          // consumer.commitAsync(); // TODO: do i need this?
        }
      }
    } catch (Exception e) {
      log.error("Consumer Error.", e);
    } finally {
      consumer.close();
    }
  }
}
