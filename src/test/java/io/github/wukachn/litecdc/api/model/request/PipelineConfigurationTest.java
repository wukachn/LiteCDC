package io.github.wukachn.litecdc.api.model.request;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.wukachn.litecdc.api.model.request.database.SourceConfiguration;
import io.github.wukachn.litecdc.api.model.request.database.mysql.MySQLSinkType;
import io.github.wukachn.litecdc.api.model.request.database.mysql.MySqlDestinationConfiguration;
import io.github.wukachn.litecdc.engine.exception.SourceValidationException;
import io.github.wukachn.litecdc.engine.exception.ValidationException;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class PipelineConfigurationTest {

  static Stream<Arguments> passingTopicStrategyAndSinkType() {
    return Stream.of(
        Arguments.of(TopicStrategy.SINGLE, MySQLSinkType.TRANSACTIONAL),
        Arguments.of(TopicStrategy.PER_TABLE, MySQLSinkType.BATCHING));
  }

  static Stream<Arguments> failingTopicStrategyAndSinkType() {
    return Stream.of(
        Arguments.of(TopicStrategy.SINGLE, MySQLSinkType.BATCHING),
        Arguments.of(TopicStrategy.PER_TABLE, MySQLSinkType.TRANSACTIONAL));
  }

  @ParameterizedTest
  @MethodSource("passingTopicStrategyAndSinkType")
  public void passValidation_compatibility(TopicStrategy topicStrategy, MySQLSinkType mySqlSinkType)
      throws SQLException, SourceValidationException, ValidationException {
    var source = mock(SourceConfiguration.class);
    var destination = mock(MySqlDestinationConfiguration.class);
    var kafka = mock(KafkaConfiguration.class);
    var email = Mockito.mock(EmailConfiguration.class);
    doNothing().when(source).validate();
    when(kafka.getTopicStrategy()).thenReturn(topicStrategy);
    doNothing().when(destination).validate();
    when(destination.getSinkType()).thenReturn(mySqlSinkType);
    doNothing().when(email).validate();

    var config =
        PipelineConfiguration.builder()
            .sourceConfig(source)
            .destinationConfig(destination)
            .kafkaConfig(kafka)
            .emailConfig(email)
            .build();

    config.validate();

    verify(source, times(1)).validate();
    verify(destination, times(1)).validate();
    verify(email, times(1)).validate();
  }

  @ParameterizedTest
  @MethodSource("failingTopicStrategyAndSinkType")
  public void failsValidation_compatibility(
      TopicStrategy topicStrategy, MySQLSinkType mySqlSinkType)
      throws SQLException, SourceValidationException {
    var source = mock(SourceConfiguration.class);
    var destination = mock(MySqlDestinationConfiguration.class);
    var kafka = mock(KafkaConfiguration.class);
    var email = Mockito.mock(EmailConfiguration.class);
    doNothing().when(source).validate();
    when(kafka.getTopicStrategy()).thenReturn(topicStrategy);
    doNothing().when(destination).validate();
    when(destination.getSinkType()).thenReturn(mySqlSinkType);
    doNothing().when(email).validate();

    var config =
        PipelineConfiguration.builder()
            .sourceConfig(source)
            .destinationConfig(destination)
            .kafkaConfig(kafka)
            .emailConfig(email)
            .build();

    assertThrows(ValidationException.class, () -> config.validate());
  }
}
