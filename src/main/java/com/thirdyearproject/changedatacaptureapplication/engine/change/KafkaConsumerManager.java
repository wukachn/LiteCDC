package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public class KafkaConsumerManager {
  private static ExecutorService executor = Executors.newFixedThreadPool(10);

  public static void createConsumers(
      String bootstrapServer,
      String topicPrefix,
      Set<TableIdentifier> tables,
      ChangeEventProcessor eventProcessor) {
    createTopicsIfNotExists(bootstrapServer, topicPrefix, tables);
    var groupedBySchema =
        tables.stream().collect(Collectors.groupingBy(TableIdentifier::getSchema)).values().stream()
            .toList();
    for (var schemaTables : groupedBySchema) {
      // Consumer per Schema.
      executor.submit(
          new ChangeDataConsumer(bootstrapServer, topicPrefix, schemaTables, eventProcessor));
    }
  }

  private static void createTopicsIfNotExists(
      String bootstrapServer, String topicPrefix, Set<TableIdentifier> tables) {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      for (var table : tables) {
        var topicName = topicPrefix + "." + table.getStringFormat();
        boolean topicExists = adminClient.listTopics().names().get().contains(topicName);
        if (!topicExists) {
          // TODO: Probably use different defaults...
          NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
          adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        }
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }
}
