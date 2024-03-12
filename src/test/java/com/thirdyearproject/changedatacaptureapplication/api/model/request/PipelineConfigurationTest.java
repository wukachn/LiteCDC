package com.thirdyearproject.changedatacaptureapplication.api.model.request;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.SourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySQLSinkType;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlDestinationConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.ValidationException;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PipelineConfigurationTest {

  @ParameterizedTest
  @MethodSource("passingTopicStrategyAndSinkType")
  public void passValidation_compatibility(TopicStrategy topicStrategy, MySQLSinkType mySqlSinkType) throws SQLException {
    var source = mock(SourceConfiguration.class);
    var destination = mock(MySqlDestinationConfiguration.class);
    var kafka = mock(KafkaConfiguration.class);
    var email = mock(EmailConfiguration.class);
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
  public void failsValidation_compatibility(TopicStrategy topicStrategy, MySQLSinkType mySqlSinkType) throws SQLException {
    var source = mock(SourceConfiguration.class);
    var destination = mock(MySqlDestinationConfiguration.class);
    var kafka = mock(KafkaConfiguration.class);
    var email = mock(EmailConfiguration.class);
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
}
