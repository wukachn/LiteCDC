package io.github.wukachn.litecdc.engine.kafka;

import io.github.wukachn.litecdc.api.model.request.TopicStrategy;
import io.github.wukachn.litecdc.engine.change.model.ChangeEvent;
import io.github.wukachn.litecdc.engine.change.model.TableIdentifier;
import io.github.wukachn.litecdc.engine.consume.ChangeEventSink;
import io.github.wukachn.litecdc.engine.exception.ConsumerException;
import io.github.wukachn.litecdc.engine.exception.ConsumerExceptionHandler;
import io.github.wukachn.litecdc.engine.kafka.serialization.ChangeEventDeserializer;
import io.github.wukachn.litecdc.engine.metrics.MetricsService;
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

  private final String bootstrapServer;
  private final String topicPrefix;
  private final List<TableIdentifier> tables;
  private final ChangeEventSink eventProcessor;
  private final MetricsService metricsService;
  private final TopicStrategy topicStrategy;
  private final ConsumerExceptionHandler consumerExceptionHandler;
  // Need a flag, a real interrupt causes issues with interrupting KafkaConsumer.
  private boolean isSoftInterrupted;

  public ChangeDataConsumer(
      String bootstrapServer,
      String topicPrefix,
      List<TableIdentifier> tables,
      ChangeEventSink eventProcessor,
      MetricsService metricsService,
      TopicStrategy topicStrategy,
      ConsumerExceptionHandler consumerExceptionHandler) {
    this.bootstrapServer = bootstrapServer;
    this.tables = tables;
    this.topicStrategy = topicStrategy;
    this.topicPrefix = topicPrefix;
    this.eventProcessor = eventProcessor;
    this.metricsService = metricsService;
    this.consumerExceptionHandler = consumerExceptionHandler;
    this.isSoftInterrupted = false;
  }

  public void softInterrupt() {
    this.isSoftInterrupted = true;
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
    try (var consumer = createConsumer(bootstrapServer)) {
      List<String> topics;
      if (topicStrategy == TopicStrategy.PER_TABLE) {
        topics = tables.stream().map(table -> topicPrefix + "." + table.getStringFormat()).toList();
      } else {
        topics = List.of(topicPrefix + ".all_tables");
      }

      consumer.subscribe(topics);
      while (!isSoftInterrupted) {
        ConsumerRecords<String, ChangeEvent> consumerRecords =
            consumer.poll(Duration.of(2000, ChronoUnit.MILLIS));
        List<ChangeEvent> changeEvents =
            StreamSupport.stream(consumerRecords.spliterator(), true)
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());
        if (!changeEvents.isEmpty()) {
          eventProcessor.process(changeEvents);
          metricsService.consumeEvents(changeEvents);
          // consumer.commitAsync(); // TODO: do i need this?
        }
      }
    } catch (Exception e) {
      consumerExceptionHandler.handle(new ConsumerException("Consumer Error.", e));
    } finally {
      log.info("Consumer closed.");
    }
  }
}
