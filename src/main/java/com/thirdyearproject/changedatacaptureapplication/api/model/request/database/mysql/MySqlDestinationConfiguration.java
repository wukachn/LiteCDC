package com.thirdyearproject.changedatacaptureapplication.api.model.request.database.mysql;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.database.DestinationConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate.ChangeEventSink;
import com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate.MySqlBatchingSink;
import com.thirdyearproject.changedatacaptureapplication.engine.consume.replicate.MySqlTransactionalSink;
import java.sql.SQLException;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

@Value
@Builder
@Jacksonized
@Slf4j
public class MySqlDestinationConfiguration implements DestinationConfiguration {
  @NonNull MySqlConnectionConfiguration connectionConfig;
  @NonNull MySQLSinkType sinkType;

  @Override
  public ChangeEventSink createChangeEventSink() {
    if (sinkType == MySQLSinkType.TRANSACTIONAL) {
      return new MySqlTransactionalSink(connectionConfig);
    } else {
      return new MySqlBatchingSink(connectionConfig);
    }
  }

  @Override
  public void validate() throws SQLException {
    var jdbcConnection = new JdbcConnection(connectionConfig);
    try (var conn = jdbcConnection.getConnection()) {}
  }
}
