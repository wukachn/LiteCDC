import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.thirdyearproject.changedatacaptureapplication.ChangeDataCaptureApplication;
import com.thirdyearproject.changedatacaptureapplication.api.PipelineController;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.PipelineStatus;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
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
public abstract class EndToEndIT {

  @ClassRule
  public static final MySQLContainer<?> mysqlContainer =
      new MySQLContainer<>("mysql:latest")
          .withUsername("mysql_user")
          .withPassword("mysql_password");
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

  protected abstract PipelineConfiguration getPipelineConfig();

  @Test
  public void endToEnd() {
    var config = getPipelineConfig();

    assertEquals(PipelineStatus.NOT_RUNNING, pipelineController.getPipelineStatus().getStatus());

    try (var conn1 =
            DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
        var conn2 =
            DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
        var conn3 =
            DriverManager.getConnection(
                postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword())) {
      conn1.setAutoCommit(false);
      conn2.setAutoCommit(false);
      conn3.setAutoCommit(false);

      try (var statement1 = conn1.createStatement()) {
        statement1.execute(
            "INSERT INTO public.newtable2 (position, name) VALUES (500, 'five hundo')");
        pipelineController.runPipeline(config);
        conn1.commit();

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
                        PipelineStatus.STREAMING,
                        pipelineController.getPipelineStatus().getStatus()));

        // Changes replicated to MySQL for snapshot events.
        await().atMost(ofSeconds(10)).untilAsserted(() -> assertTrue(areContentsEqual()));
        try (var statement2 = conn2.createStatement();
            var statement3 = conn3.createStatement()) {
          statement2.execute(
              "INSERT INTO public.newtable2 (position, name) VALUES (600, 'six hundo')");
          statement3.execute(
              "INSERT INTO public.newtable2 (position, name) VALUES (700, 'seven hundo')");
          statement1.execute(
              "UPDATE public.newtable2 SET \"name\"='updated1' WHERE \"position\"=100");
          conn1.commit();
          statement2.execute(
              "UPDATE public.newtable2 SET \"name\"='updated2' WHERE \"position\"=100");
          conn2.commit();
          statement1.execute(
              "UPDATE public.newtable2 SET \"name\"='updated1again' WHERE \"position\"=100");
          conn1.commit();
          statement1.execute(
              "INSERT INTO public.newtable2 (position, name) VALUES (800, 'eight hundo')");
          statement3.execute("DELETE FROM public.newtable2 WHERE \"position\"=100");
          conn3.commit();
          statement1.execute("UPDATE public.newtable2 SET \"name\"='up3' WHERE \"position\"=100");
          conn1.commit();
          statement3.execute(
              "INSERT INTO public.newtable2 (position, name) VALUES (900, 'nine hundo')");
          statement3.execute("DELETE FROM public.newtable2 WHERE \"position\"=200");
          conn3.commit();
          statement1.execute(
              "UPDATE public.newtable2 SET \"name\"='updated200' WHERE \"position\"=200 OR \"position\"=300");
          conn1.commit();
        }
      }
    } catch (SQLException e) {
      log.error("Failed.", e);
      fail();
    }
    await().atMost(ofSeconds(10)).untilAsserted(() -> assertTrue(areContentsEqual()));
  }

  private boolean areContentsEqual() {
    log.info("Are contents equal.");
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
      var mysqlResultSet =
          mysqlStatement.executeQuery("SELECT * FROM cdc_public.newtable2 ORDER BY \"position\"");
      var postgresResultSet =
          postgresStatement.executeQuery("SELECT * FROM public.newtable2 ORDER BY \"position\"");
      while (true) {
        postgresResultSet.next();
        mysqlResultSet.next();
        if (mysqlResultSet.isAfterLast() || postgresResultSet.isAfterLast()) {
          break;
        }
        log.info("------");
        log.info(
            postgresResultSet.getInt("position") + " ---- " + mysqlResultSet.getInt("position"));
        log.info(postgresResultSet.getString("name") + " ---- " + mysqlResultSet.getString("name"));
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
