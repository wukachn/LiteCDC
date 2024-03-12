package com.thirdyearproject.changedatacaptureapplication.engine.kafka;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.TopicStrategy;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeDataConsumer;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate.ChangeEventSink;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.ConsumerExceptionHandler;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public class KafkaConsumerManager {
  private static final List<ChangeDataConsumer> consumers = new ArrayList<>();

  public static void closeConsumers() {
    log.info("Attempting to close consumers.");
    for (var consumer : consumers) {
      consumer.softInterrupt();
    }
    consumers.clear();
  }

  public static void createConsumers(
      String bootstrapServer,
      String topicPrefix,
      Set<TableIdentifier> tables,
      ChangeEventSink eventProcessor,
      MetricsService metricsService,
      TopicStrategy topicStrategy,
      ConsumerExceptionHandler consumerExceptionHandler) {
    createTopicsIfNotExists(bootstrapServer, topicPrefix, tables, topicStrategy);

    if (topicStrategy == TopicStrategy.PER_TABLE) {
      var groupedBySchema =
          tables.stream()
              .collect(Collectors.groupingBy(TableIdentifier::getSchema))
              .values()
              .stream()
              .toList();
      for (var schemaTables : groupedBySchema) {
        // Consumer per Schema.
        var consumer =
            new ChangeDataConsumer(
                bootstrapServer,
                topicPrefix,
                schemaTables,
                eventProcessor,
                metricsService,
                topicStrategy,
                consumerExceptionHandler);
        consumers.add(consumer);
        new Thread(consumer).start();
      }
    } else if (topicStrategy == TopicStrategy.SINGLE) {
      var allTables = tables.stream().toList();
      var consumer =
          new ChangeDataConsumer(
              bootstrapServer,
              topicPrefix,
              allTables,
              eventProcessor,
              metricsService,
              topicStrategy,
              consumerExceptionHandler);
      consumers.add(consumer);
      new Thread(consumer).start();
    }
  }

  private static void createTopicsIfNotExists(
      String bootstrapServer,
      String topicPrefix,
      Set<TableIdentifier> tables,
      TopicStrategy topicStrategy) {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      if (topicStrategy == TopicStrategy.PER_TABLE) {
        for (var table : tables) {
          var topicName = topicPrefix + "." + table.getStringFormat();
          createTopic(adminClient, topicName);
        }
      } else if (topicStrategy == TopicStrategy.SINGLE) {
        var topicName = topicPrefix + ".all_tables";
        createTopic(adminClient, topicName);
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  private static void createTopic(AdminClient adminClient, String topicName)
      throws ExecutionException, InterruptedException {
    boolean topicExists = adminClient.listTopics().names().get().contains(topicName);
    if (!topicExists) {
      NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
      adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
    }
  }
}
