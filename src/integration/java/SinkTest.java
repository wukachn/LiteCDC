import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.thirdyearproject.changedatacaptureapplication.ChangeDataCaptureApplication;
import com.thirdyearproject.changedatacaptureapplication.api.PipelineController;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.KafkaConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.TopicStrategy;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySQLSinkType;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlDestinationConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresSourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.PipelineStatus;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(classes = ChangeDataCaptureApplication.class)
@RunWith(SpringRunner.class)
@Slf4j
public class SinkTest {

  static KafkaContainer kafkaContainer;

  private static DockerImageName myImage =
      DockerImageName.parse("debezium/postgres:16-alpine").asCompatibleSubstituteFor("postgres");

  @ClassRule
  public static final PostgreSQLContainer<?> postgresContainer =
      new PostgreSQLContainer<>(myImage)
          .withDatabaseName("db")
          .withUsername("user")
          .withPassword("pass");

  @ClassRule
  public static final MySQLContainer<?> mysqlContainer =
      new MySQLContainer<>("mysql:latest")
          .withUsername("mysql_user")
          .withPassword("mysql_password");

  @Autowired private PipelineController pipelineController;

  @BeforeClass
  public static void beforeClass() {
    kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
    kafkaContainer.start();

    try (var statement =
        DriverManager.getConnection(postgresContainer.getJdbcUrl(), "user", "pass")
            .createStatement()) {
      String sqlFilePath = "db_setup/setup_postgres.sql";
      String sqlString = new String(Files.readAllBytes(Paths.get(sqlFilePath)));
      statement.execute(sqlString);
      log.info("Created tables");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try (var statement =
        DriverManager.getConnection(
                mysqlContainer.getJdbcUrl(), "root", mysqlContainer.getPassword())
            .createStatement()) {
      String sqlFilePath = "db_setup/setup_mysql.sql";
      String sqlString = new String(Files.readAllBytes(Paths.get(sqlFilePath)));
      var sqlArray = sqlString.split(";");
      for (var sql : sqlArray) {
        statement.execute(sql);
      }
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

  @Test
  public void test() throws InterruptedException {
    var config =
        PipelineConfiguration.builder()
            .kafkaConfig(
                KafkaConfiguration.builder()
                    .bootstrapServer(kafkaContainer.getBootstrapServers())
                    .topicPrefix("thirdyearproject")
                    .topicStrategy(TopicStrategy.SINGLE)
                    .build())
            .sourceConfig(
                PostgresSourceConfiguration.builder()
                    .connectionConfig(
                        PostgresConnectionConfiguration.builder()
                            .database(postgresContainer.getDatabaseName())
                            .port(postgresContainer.getFirstMappedPort())
                            .host(postgresContainer.getHost())
                            .user(postgresContainer.getUsername())
                            .password(postgresContainer.getPassword())
                            .build())
                    .capturedTables(Set.of(TableIdentifier.of("public", "newtable2")))
                    .build())
            .destinationConfig(
                MySqlDestinationConfiguration.builder()
                    .connectionConfig(
                        MySqlConnectionConfiguration.builder()
                            .host(mysqlContainer.getHost())
                            .port(mysqlContainer.getFirstMappedPort())
                            .user(mysqlContainer.getUsername())
                            .password(mysqlContainer.getPassword())
                            .build())
                    .sinkType(MySQLSinkType.TRANSACTIONAL)
                    .build())
            .build();

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

    // Changes replicated to MySQL for snapshot events.
    await().atMost(ofSeconds(10)).untilAsserted(() -> assertTrue(areContentsEqual()));

    try (var conn1 =
            DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
        var conn2 =
            DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword())) {
      conn1.setAutoCommit(false);
      conn2.setAutoCommit(false);
      try (var statement1 = conn1.createStatement();
          var statement2 = conn2.createStatement()) {
        statement1.execute(
            "INSERT INTO public.newtable2 (position, name) VALUES (500, 'five hundo')");
        statement2.execute(
            "INSERT INTO public.newtable2 (position, name) VALUES (600, 'six hundo')");
        conn1.commit();
        conn2.commit();
      }

    } catch (SQLException e) {
      log.error("Failed.", e);
      fail();
    }

    await().atMost(ofSeconds(10)).untilAsserted(() -> assertTrue(areContentsEqual()));
  }

  private boolean areContentsEqual() {
    try (var mysqlConnection =
            DriverManager.getConnection(
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
        var mysqlStatement = mysqlConnection.createStatement();
        var postgresConnection =
            DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
        var postgresStatement = postgresConnection.createStatement()) {
      var mysqlResultSet = mysqlStatement.executeQuery("SELECT * FROM cdc_public.newtable2");
      var postgresResultSet = postgresStatement.executeQuery("SELECT * FROM public.newtable2");
      while (true) {
        postgresResultSet.next();
        mysqlResultSet.next();
        if (mysqlResultSet.isAfterLast() || postgresResultSet.isAfterLast()) {
          break;
        }
        if (postgresResultSet.getInt("position") != mysqlResultSet.getInt("position")
            || !postgresResultSet.getString("name").equals(mysqlResultSet.getString("name"))) {
          return false;
        }
      }
      return mysqlResultSet.isAfterLast() && postgresResultSet.isAfterLast();
    } catch (Exception e) {
      log.error("Failed to compare contents.", e);
      return false;
    }
  }
}
