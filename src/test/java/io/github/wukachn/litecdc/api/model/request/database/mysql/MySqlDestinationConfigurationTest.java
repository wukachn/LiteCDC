package io.github.wukachn.litecdc.api.model.request.database.mysql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.github.wukachn.litecdc.engine.consume.replicate.MySqlBatchingSink;
import io.github.wukachn.litecdc.engine.consume.replicate.MySqlSink;
import io.github.wukachn.litecdc.engine.consume.replicate.MySqlTransactionalSink;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.MySQLContainer;

@DirtiesContext
public class MySqlDestinationConfigurationTest {

  @ClassRule
  public static final MySQLContainer<?> mysqlContainer =
      new MySQLContainer<>("mysql:latest")
          .withUsername("mysql_user")
          .withPassword("mysql_password");

  static Stream<Arguments> sinkPairs() {
    return Stream.of(
        Arguments.of(MySQLSinkType.TRANSACTIONAL, MySqlTransactionalSink.class),
        Arguments.of(MySQLSinkType.BATCHING, MySqlBatchingSink.class));
  }

  @Test
  public void passValidationTest() {
    var conn =
        MySqlConnectionConfiguration.builder()
            .host(mysqlContainer.getHost())
            .port(mysqlContainer.getFirstMappedPort())
            .user(mysqlContainer.getUsername())
            .password("MYSQL_PASS")
            .build();
    var config =
        MySqlDestinationConfiguration.builder()
            .connectionConfig(conn)
            .sinkType(MySQLSinkType.TRANSACTIONAL)
            .build();

    assertDoesNotThrow(config::validate);
  }

  @Test
  public void failValidationTest() {
    var conn =
        MySqlConnectionConfiguration.builder()
            .host(mysqlContainer.getHost())
            .port(mysqlContainer.getFirstMappedPort())
            .user("invalid_user")
            .password("MYSQL_PASS")
            .build();
    var config =
        MySqlDestinationConfiguration.builder()
            .connectionConfig(conn)
            .sinkType(MySQLSinkType.TRANSACTIONAL)
            .build();

    assertThrows(SQLException.class, config::validate);
  }

  @ParameterizedTest
  @MethodSource("sinkPairs")
  public void createsCorrectSink(MySQLSinkType sinkType, Class<? extends MySqlSink> expectedClass) {
    var connMock = mock(MySqlConnectionConfiguration.class);
    var config =
        MySqlDestinationConfiguration.builder()
            .connectionConfig(connMock)
            .sinkType(sinkType)
            .build();
    assertEquals(expectedClass, config.createChangeEventSink().getClass());
  }
}
