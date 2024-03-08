import com.thirdyearproject.changedatacaptureapplication.api.model.request.KafkaConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.TopicStrategy;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySQLSinkType;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql.MySqlDestinationConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresConnectionConfiguration;
import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.postgres.PostgresSourceConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.util.Set;

public class EndToEndBatchingIT extends EndToEndIT {
  @Override
  protected PipelineConfiguration getPipelineConfig() {
    return PipelineConfiguration.builder()
        .kafkaConfig(
            KafkaConfiguration.builder()
                .bootstrapServer(kafkaContainer.getBootstrapServers())
                .topicPrefix("thirdyearproject")
                .topicStrategy(TopicStrategy.PER_TABLE)
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
                .sinkType(MySQLSinkType.BATCHING)
                .build())
        .build();
  }
}
