import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.thirdyearproject.changedatacaptureapplication.ChangeDataCaptureApplication;
import com.thirdyearproject.changedatacaptureapplication.api.PipelineController;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.serialization.ChangeEventDeserializer;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.PipelineStatus;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(classes = ChangeDataCaptureApplication.class)
@RunWith(SpringRunner.class)
@Slf4j
public abstract class NoDestinationIT {

  static KafkaContainer kafkaContainer;

  private static DockerImageName myImage =
      DockerImageName.parse("debezium/postgres:16-alpine").asCompatibleSubstituteFor("postgres");

  @ClassRule
  public static final PostgreSQLContainer<?> postgresContainer =
      new PostgreSQLContainer<>(myImage)
          .withDatabaseName("db")
          .withUsername("user")
          .withPassword("pass");

  @Autowired private PipelineController pipelineController;

  @BeforeClass
  public static void beforeClass() {
    kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
    kafkaContainer.start();

    // Get connection to the PostgreSQL container
    var connection =
        new JdbcConnection(
            PostgresConnectionConfiguration.builder()
                .database(postgresContainer.getDatabaseName())
                .port(postgresContainer.getFirstMappedPort())
                .host(postgresContainer.getHost())
                .user(postgresContainer.getUsername())
                .password(postgresContainer.getPassword())
                .build());

    try (var statement = connection.getConnection().createStatement()) {
      String sqlFilePath = "db_setup/setup_postgres.sql";
      String sqlString = new String(Files.readAllBytes(Paths.get(sqlFilePath)));
      statement.execute(sqlString);
      log.info("Created tables");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void afterClass() {
    kafkaContainer.stop();
  }

  protected abstract PipelineConfiguration getPipelineConfig();

  protected abstract List<String> getTopics();

  @Test
  public void test() {
    var config = getPipelineConfig();

    assertEquals(PipelineStatus.NOT_RUNNING, pipelineController.getPipelineStatus().getStatus());

    pipelineController.runPipeline(config);

    await()
        .atMost(ofSeconds(2))
        .untilAsserted(
            () ->
                assertEquals(
                    PipelineStatus.SNAPSHOTTING,
                    pipelineController.getPipelineStatus().getStatus()));

    await()
        .atMost(ofSeconds(2))
        .untilAsserted(
            () ->
                assertEquals(
                    PipelineStatus.STREAMING, pipelineController.getPipelineStatus().getStatus()));

    assertEquals(List.of(100L, 200L, 300L, 400L, 1L, 2L, 3L), getIdsFromKafka());
  }

  private List<Long> getIdsFromKafka() {
    var ids = new ArrayList<Long>();
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "test-group" + UUID.randomUUID());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ChangeEventDeserializer.class.getName());
    var i = 0;
    try (Consumer<String, ChangeEvent> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(getTopics());
      ConsumerRecords<String, ChangeEvent> records = consumer.poll(Duration.ofMillis(1000));
      for (ConsumerRecord<String, ChangeEvent> record : records) {
        var after = record.value().getAfter();
        if (after != null) {
          ids.add((Long) after.get(0).getValue());
        }
      }
    }
    return ids;
  }
}
