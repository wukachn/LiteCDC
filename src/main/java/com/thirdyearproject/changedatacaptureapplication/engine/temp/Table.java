package com.thirdyearproject.changedatacaptureapplication.engine.temp;

import com.thirdyearproject.changedatacaptureapplication.engine.util.TypeConverter;
import java.util.List;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

@Value
@Builder
public class Table {
  TableIdentifier tableIdentifier;
  List<Column> columns;

  public Schema getSchema() {
    var structSchemaBuilder = SchemaBuilder.struct().name(tableIdentifier.getStringFormat());
    for (var column : columns) {
      var columnName = column.getName();
      var type = column.getType();
      var isNullable = column.isNullable();
      // TODO: Look into column length, should i be concerned if that data is lost.
      var fieldSchema = TypeConverter.sqlColumnToKafkaConnectType(type, isNullable);
      structSchemaBuilder.field(columnName, fieldSchema);
    }
    return structSchemaBuilder.build();
  }
}
