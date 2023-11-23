package com.thirdyearproject.changedatacaptureapplication.engine.util;

import java.sql.Types;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

@Slf4j
public class TypeConverter {
  public static Schema sqlColumnToKafkaConnectType(int sqlColumnType, boolean isNullable) {
    // Small subset of types for now. TODO: Expand to more types.
    var schemaBuilder =
        switch (sqlColumnType) {
          case Types.BOOLEAN, Types.BIT -> SchemaBuilder.bool();
          case Types.TINYINT -> SchemaBuilder.int8();
          case Types.SMALLINT -> SchemaBuilder.int16();
          case Types.INTEGER -> SchemaBuilder.int32();
          case Types.BIGINT -> SchemaBuilder.int64();
          case Types.REAL -> SchemaBuilder.float32();
          case Types.DOUBLE -> SchemaBuilder.float64();
          default -> SchemaBuilder.string();
        };
    if (isNullable) {
      schemaBuilder = schemaBuilder.optional();
    }
    return schemaBuilder.build();
  }
}
