package com.thirdyearproject.changedatacaptureapplication.engine.change;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public class KafkaConsumerManager {
  private static ExecutorService executor = Executors.newFixedThreadPool(10);

  public static void createConsumers(
      String bootstrapServer,
      String prefix,
      Set<TableIdentifier> tables,
      ChangeEventProcessor eventProcessor) {
    createTopicsIfNotExists(bootstrapServer, prefix, tables);
    for (var table : tables) {
      // TODO: Can this handle lots of tables?
      executor.submit(new ChangeDataConsumer(bootstrapServer, prefix, table, eventProcessor));
    }
  }

  private static void createTopicsIfNotExists(
      String bootstrapServer, String prefix, Set<TableIdentifier> tables) {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      for (var table : tables) {
        var topicName = prefix + "." + table.getStringFormat();
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
