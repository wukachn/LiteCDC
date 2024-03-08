import com.thirdyearproject.changedatacaptureapplication.api.model.request.KafkaConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.TopicStrategy;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresSourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.util.List;
import java.util.Set;

public class NoDestinationSingleIT extends NoDestinationIT {
  @Override
  protected PipelineConfiguration getPipelineConfig() {
    return PipelineConfiguration.builder()
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
                .capturedTables(
                    Set.of(
                        TableIdentifier.of("public", "newtable1"),
                        TableIdentifier.of("public", "newtable2")))
                .build())
        .build();
  }

  @Override
  protected List<String> getTopics() {
    return List.of("thirdyearproject.all_tables");
  }
}
