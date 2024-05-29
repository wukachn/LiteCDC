package io.github.wukachn.litecdc;

import io.github.wukachn.litecdc.api.model.request.KafkaConfiguration;
import io.github.wukachn.litecdc.api.model.request.PipelineConfiguration;
import io.github.wukachn.litecdc.api.model.request.TopicStrategy;
import io.github.wukachn.litecdc.api.model.request.database.mysql.MySQLSinkType;
import io.github.wukachn.litecdc.api.model.request.database.mysql.MySqlConnectionConfiguration;
import io.github.wukachn.litecdc.api.model.request.database.mysql.MySqlDestinationConfiguration;
import io.github.wukachn.litecdc.api.model.request.database.postgres.PostgresConnectionConfiguration;
import io.github.wukachn.litecdc.api.model.request.database.postgres.PostgresSourceConfiguration;
import io.github.wukachn.litecdc.engine.change.model.TableIdentifier;
import java.util.Set;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
public class EndToEndBatchingTest extends EndToEndTest {
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
                        .password("PG_PASS")
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
                        .password("MYSQL_PASS")
                        .build())
                .sinkType(MySQLSinkType.BATCHING)
                .build())
        .build();
  }
}
