package com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.SourceValidationException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@DirtiesContext
public class PostgresDestinationConfigurationTest {

  private static DockerImageName myImage =
      DockerImageName.parse("debezium/postgres:16-alpine").asCompatibleSubstituteFor("postgres");

  @ClassRule
  public static final PostgreSQLContainer<?> postgresContainer =
      new PostgreSQLContainer<>(myImage)
          .withDatabaseName("db")
          .withUsername("user_name")
          .withPassword("pg_password");

  @BeforeClass
  public static void beforeClass() {
    var props = new Properties();
    props.put("user", postgresContainer.getUsername());
    props.put("password", postgresContainer.getPassword());
    props.put("assumeMinServerVersion", "9.4");
    props.put("replication", "database");
    props.put("preferQueryMode", "simple");

    try (var statement =
        DriverManager.getConnection(postgresContainer.getJdbcUrl(), props).createStatement()) {
      var createTbl =
          "CREATE TABLE IF NOT EXISTS newtable1 (position INT PRIMARY KEY, name VARCHAR(16));";
      statement.execute(createTbl);
      var createPub = "CREATE PUBLICATION cdc_publication FOR ALL TABLES;";
      statement.execute(createPub);
      var createSlot = "CREATE_REPLICATION_SLOT cdc_replication_slot LOGICAL pgoutput;";
      statement.execute(createSlot);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void passValidationTest() {
    var conn =
        PostgresConnectionConfiguration.builder()
            .host(postgresContainer.getHost())
            .port(postgresContainer.getFirstMappedPort())
            .user(postgresContainer.getUsername())
            .database(postgresContainer.getDatabaseName())
            .password("PG_PASS")
            .build();
    var config =
        PostgresSourceConfiguration.builder()
            .connectionConfig(conn)
            .capturedTables(Set.of(TableIdentifier.of("public", "newtable1")))
            .publication("pub")
            .replicationSlot("slot")
            .build();

    assertDoesNotThrow(() -> config.validate());
  }

  @Test
  public void failValidationTest_existingPublication() {
    var conn =
        PostgresConnectionConfiguration.builder()
            .host(postgresContainer.getHost())
            .port(postgresContainer.getFirstMappedPort())
            .user(postgresContainer.getUsername())
            .database(postgresContainer.getDatabaseName())
            .password("PG_PASS")
            .build();
    var config =
        PostgresSourceConfiguration.builder()
            .connectionConfig(conn)
            .capturedTables(Set.of(TableIdentifier.of("public", "table1")))
            .replicationSlot("slot")
            .build();

    assertThrows(SourceValidationException.class, () -> config.validate());
  }

  @Test
  public void failValidationTest_existingSlot() {
    var conn =
        PostgresConnectionConfiguration.builder()
            .host(postgresContainer.getHost())
            .port(postgresContainer.getFirstMappedPort())
            .user(postgresContainer.getUsername())
            .database(postgresContainer.getDatabaseName())
            .password("PG_PASS")
            .build();
    var config =
        PostgresSourceConfiguration.builder()
            .connectionConfig(conn)
            .capturedTables(Set.of(TableIdentifier.of("public", "table1")))
            .publication("pub")
            .build();

    assertThrows(SourceValidationException.class, () -> config.validate());
  }

  @Test
  public void failValidationTest_missingPerms() {
    var conn =
        PostgresConnectionConfiguration.builder()
            .host(postgresContainer.getHost())
            .port(postgresContainer.getFirstMappedPort())
            .user(postgresContainer.getUsername())
            .database(postgresContainer.getDatabaseName())
            .password("PG_PASS")
            .build();
    var config =
        PostgresSourceConfiguration.builder()
            .connectionConfig(conn)
            .capturedTables(Set.of(TableIdentifier.of("public", "table1")))
            .publication("pub")
            .replicationSlot("slot")
            .build();

    assertThrows(SourceValidationException.class, () -> config.validate());
  }

  @Test
  public void getCaptureTablesTest() {
    var tableSet =
        Set.of(TableIdentifier.of("public", "table1"), TableIdentifier.of("public", "table2"));

    var conn = mock(PostgresConnectionConfiguration.class);
    var config =
        PostgresSourceConfiguration.builder()
            .connectionConfig(conn)
            .capturedTables(tableSet)
            .build();

    assertEquals(tableSet, config.getTables());
  }
}
