package com.thirdyearproject.changedatacaptureapplication.engine.util;

import java.sql.Types;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;

@Slf4j
public class TypeConverter {
  public static Schema sqlColumnToKafkaConnectType(int sqlColumnType) {
    // Small subset of types for now. TODO: Expand to more types.
    return switch (sqlColumnType) {
      case Types.BOOLEAN, Types.BIT -> Schema.BOOLEAN_SCHEMA;
      case Types.TINYINT -> Schema.INT8_SCHEMA;
      case Types.SMALLINT -> Schema.INT16_SCHEMA;
      case Types.INTEGER -> Schema.INT32_SCHEMA;
      case Types.BIGINT -> Schema.INT64_SCHEMA;
      case Types.REAL -> Schema.FLOAT32_SCHEMA;
      case Types.DOUBLE -> Schema.FLOAT64_SCHEMA;
      default -> Schema.STRING_SCHEMA;
    };
  }
}
