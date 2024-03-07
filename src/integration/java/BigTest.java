import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.thirdyearproject.changedatacaptureapplication.ChangeDataCaptureApplication;
import com.thirdyearproject.changedatacaptureapplication.api.PipelineController;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.KafkaConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.TopicStrategy;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresSourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.PipelineStatus;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
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
public class BigTest {

  static KafkaContainer kafkaContainer;
  static String topic = "test-topic";

  JdbcConnection jdbcConnection;

  private static DockerImageName myImage = DockerImageName.parse("debezium/postgres:16-alpine")
      .asCompatibleSubstituteFor("postgres");
  @ClassRule
  public static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(
      myImage)
      .withDatabaseName("db")
      .withUsername("user")
      .withPassword("pass");

  @Autowired
  private PipelineController pipelineController;

  @BeforeClass
  public static void beforeClass() {
    kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
    kafkaContainer.start();

    // Get connection to the PostgreSQL container
    var connection = new JdbcConnection(
        PostgresConnectionConfiguration.builder().database(postgresContainer.getDatabaseName())
            .port(postgresContainer.getFirstMappedPort()).host(postgresContainer.getHost())
            .user(postgresContainer.getUsername())
            .password(postgresContainer.getPassword()).build());
    log.info("befoer");
    try (var statement = connection.getConnection().createStatement()) {
      log.info("Creating tables");
      statement.execute("""
          CREATE TABLE IF NOT EXISTS newtable1 (position INT PRIMARY KEY, name VARCHAR(16));
          CREATE TABLE IF NOT EXISTS newtable2 (position INT PRIMARY KEY, name VARCHAR(16));
          CREATE TABLE Towns (
            id SERIAL UNIQUE NOT NULL,
            code VARCHAR(10) NOT NULL,
            article TEXT,
            name TEXT NOT NULL,
            department VARCHAR(4) NOT NULL,
            UNIQUE (code, department)
          );
                    
          DO $$
          DECLARE
              table_record RECORD;
          BEGIN
              FOR table_record IN
                  SELECT table_name
                  FROM information_schema.tables
                  WHERE table_schema = 'public'
              LOOP
                  EXECUTE 'ALTER TABLE ' || table_record.table_name || ' REPLICA IDENTITY FULL;';
              END LOOP;
          END $$;
                    
          INSERT INTO newtable1 VALUES (1, 'one');
          INSERT INTO newtable1 VALUES (2, 'two');
          INSERT INTO newtable1 VALUES (3, 'three');
                    
          INSERT INTO newtable2 VALUES (100, 'one hundo');
          INSERT INTO newtable2 VALUES (200, 'two hundo');
          INSERT INTO newtable2 VALUES (300, 'three hundo');
          INSERT INTO newtable2 VALUES (400, 'four hundo');""");
      log.info("Created tables");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void afterClass() {
    kafkaContainer.stop();
  }

  @Test
  public void test() throws Exception {
    var config = PipelineConfiguration.builder().kafkaConfig(
            KafkaConfiguration.builder().bootstrapServer(kafkaContainer.getBootstrapServers())
                .topicPrefix("thirdyearproject").topicStrategy(
                    TopicStrategy.SINGLE).build())
        .sourceConfig(PostgresSourceConfiguration.builder().connectionConfig(
            PostgresConnectionConfiguration.builder().database(postgresContainer.getDatabaseName())
                .port(postgresContainer.getFirstMappedPort()).host(postgresContainer.getHost())
                .user(postgresContainer.getUsername())
                .password(postgresContainer.getPassword()).build()).capturedTables(Set.of(
            TableIdentifier.of("public", "newtable1"))).build())
        .build();

    assertEquals(PipelineStatus.NOT_RUNNING, pipelineController.getPipelineStatus().getStatus());
    pipelineController.runPipeline(config);
    await().atMost(ofSeconds(2)).untilAsserted(() -> assertEquals(PipelineStatus.SNAPSHOTTING,
        pipelineController.getPipelineStatus().getStatus()));
    Thread.sleep(5000);
  }
}
