package com.thirdyearproject.changedatacaptureapplication.api.model.request;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.DestinationConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.SourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySQLSinkType;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlDestinationConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.PipelineConfigurationException;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.ValidationException;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

@Slf4j
@Value
@Jacksonized
@Builder
@EqualsAndHashCode
public class PipelineConfiguration {
  @NonNull KafkaConfiguration kafkaConfig;
  @NonNull SourceConfiguration sourceConfig;
  @Nullable DestinationConfiguration destinationConfig;

  public void validate() throws ValidationException {
    log.info("Validating pipeline.");
    try {
      log.info("Validating source.");
      sourceConfig.validate();

      if (destinationConfig != null) {
        log.info("Validating destination.");
        destinationConfig.validate();

        log.info("Validating sink compatibility.");
        validateSinkCompatibility();
      }
    } catch (Exception e) {
      log.info("Pipeline has failed validation.", e);
      throw new ValidationException("Pipeline has failed validation.", e);
    }
    log.info("Pipeline has passed initial validation.");
  }

  private void validateSinkCompatibility() {
    if (destinationConfig instanceof MySqlDestinationConfiguration) {
      var topicStrategy = kafkaConfig.getTopicStrategy();
      var sinkType = ((MySqlDestinationConfiguration) destinationConfig).getSinkType();

      if (sinkType == MySQLSinkType.BATCHING && topicStrategy == TopicStrategy.SINGLE) {
        throw new PipelineConfigurationException(
            "Incompatible configuration: Cannot use a single topic for a batching sink.");
      }

      if (sinkType == MySQLSinkType.TRANSACTIONAL && topicStrategy == TopicStrategy.PER_TABLE) {
        throw new PipelineConfigurationException(
            "Incompatible configuration: Cannot use a topic per table for your transactional sink.");
      }
    }
  }
}
