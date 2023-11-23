// This file is heavily inspired by debezium
// https://github.com/debezium/debezium/blob/ef69dbe91c482d4ea505368b3a1a55c40eeb5ffb/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/pgoutput/PgOutputMessageDecoder.java#L722
package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.temp.Table;
import com.thirdyearproject.changedatacaptureapplication.engine.temp.TableIdentifier;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.postgresql.replication.LogSequenceNumber;

@Slf4j
public class PgOutputMessageDecoder {

  private static final Instant PG_EPOCH =
      LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
  private static final Map<Integer, Table> relationIdToTableId = new HashMap();
  private static final Schema METADATA_SCHEMA =
      SchemaBuilder.struct()
          .field("lsn", Schema.INT64_SCHEMA)
          .field("tableIdentifier", Schema.STRING_SCHEMA)
          .field("operation", Schema.STRING_SCHEMA)
          .field("transactionId", Schema.INT64_SCHEMA)
          .field("transactionCommitTime", Schema.INT64_SCHEMA)
          .name("metadata")
          .build();
  private static final Schema EMPTY_SCHEMA = SchemaBuilder.struct().build();
  private JdbcConnection jdbcConnection;
  private Long transactionId;
  private Instant transactionCommitTime;

  public PgOutputMessageDecoder(JdbcConnection jdbcConnection) {
    this.jdbcConnection = jdbcConnection;
  }

  public Optional<ChangeEvent> processNotEmptyMessage(ByteBuffer buffer, LogSequenceNumber lsn)
      throws SQLException {
    final MessageType messageType = MessageType.forType((char) buffer.get());
    switch (messageType) {
      case BEGIN:
        handleBeginMessage(buffer);
        break;
      case RELATION:
        handleRelationMessage(buffer);
        break;
      case INSERT:
        return decodeInsert(buffer, lsn);
      case UPDATE:
        return decodeUpdate(buffer, lsn);
      case DELETE:
        return decodeDelete(buffer, lsn);
      default:
        break;
    }
    return Optional.empty();
  }

  private void handleBeginMessage(ByteBuffer buffer) throws SQLException {
    var lsn = buffer.getLong();
    this.transactionCommitTime = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
    this.transactionId = Integer.toUnsignedLong(buffer.getInt());
  }

  private void handleRelationMessage(ByteBuffer buffer) throws SQLException {
    int relationId = buffer.getInt();

    var schemaName = readStringFromBuffer(buffer);
    var tableName = readStringFromBuffer(buffer);
    var tableId = TableIdentifier.of(schemaName, tableName);
    var columns = jdbcConnection.getTableColumns(tableId);
    var table = Table.builder().tableIdentifier(tableId).columns(columns).build();

    relationIdToTableId.put(relationId, table);
  }

  private Optional<ChangeEvent> decodeInsert(ByteBuffer buffer, LogSequenceNumber lsn) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get();

    var table = relationIdToTableId.get(relationId);

    var metadata = new Struct(METADATA_SCHEMA);
    metadata.put("lsn", lsn.asLong());
    metadata.put("tableIdentifier", table.getTableIdentifier().getStringFormat());
    metadata.put("operation", "create");
    metadata.put("transactionId", transactionId);
    metadata.put("transactionCommitTime", transactionCommitTime.getEpochSecond());

    var before = new Struct(EMPTY_SCHEMA);

    var after = buildColumnData(table, buffer);

    var changeEvent = ChangeEvent.builder().metadata(metadata).before(before).after(after).build();
    return Optional.of(changeEvent);
  }

  private Optional<ChangeEvent> decodeUpdate(ByteBuffer buffer, LogSequenceNumber lsn) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get(); // N or (O or K)

    var table = relationIdToTableId.get(relationId);

    var metadata = new Struct(METADATA_SCHEMA);
    metadata.put("lsn", lsn.asLong());
    metadata.put("tableIdentifier", table.getTableIdentifier().getStringFormat());
    metadata.put("transactionId", transactionId);
    metadata.put("transactionCommitTime", transactionCommitTime.getEpochSecond());
    metadata.put("operation", "update");

    Struct before;
    // Read before values if available. Otherwise, leave before empty.
    if ('O' == tupleType || 'K' == tupleType) {
      before = buildColumnData(table, buffer);

      // Increment position
      tupleType = (char) buffer.get();
    } else {
      before = new Struct(EMPTY_SCHEMA);
    }

    var after = buildColumnData(table, buffer);

    var changeEvent = ChangeEvent.builder().metadata(metadata).before(before).after(after).build();

    return Optional.of(changeEvent);
  }

  private Optional<ChangeEvent> decodeDelete(ByteBuffer buffer, LogSequenceNumber lsn) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get();

    var table = relationIdToTableId.get(relationId);

    var metadata = new Struct(METADATA_SCHEMA);
    metadata.put("lsn", lsn.asLong());
    metadata.put("tableIdentifier", table.getTableIdentifier().getStringFormat());
    metadata.put("transactionId", transactionId);
    metadata.put("transactionCommitTime", transactionCommitTime.getEpochSecond());
    metadata.put("operation", "delete");

    var before = buildColumnData(table, buffer);

    var after = new Struct(EMPTY_SCHEMA);

    var changeEvent = ChangeEvent.builder().metadata(metadata).before(before).after(after).build();

    return Optional.of(changeEvent);
  }

  private Struct buildColumnData(Table table, ByteBuffer buffer) {
    try {
      Map<String, Object> tupleMap = (Map<String, Object>) parseTupleData(buffer);

      var values = (String) tupleMap.get("values");
      var csv = values.substring(1, values.length() - 1);
      String[] valuesArray = csv.split(",");

      var tableSchema = table.getSchema();
      var tableFields = tableSchema.fields();
      var dataStruct = new Struct(tableSchema);
      var i = 0;
      for (var field : tableFields) {
        var name = field.schema().type().getName();
        if (name.equals("string")) {
          dataStruct.put(field, valuesArray[i]);
        } else if (name.equals("float32")) {
          dataStruct.put(field, Float.valueOf(valuesArray[i]));
        } else if (name.equals("float64")) {
          dataStruct.put(field, Double.valueOf(valuesArray[i]));
        } else if (name.equals("boolean")) {
          dataStruct.put(field, Boolean.valueOf(valuesArray[i]));
        } else {
          dataStruct.put(field, Integer.valueOf(valuesArray[i]));
        }
        i++;
      }
      return dataStruct;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Source:
  // https://github.com/davyam/pgEasyReplication/blob/master/src/main/java/com/dg/easyReplication/Decode.java#L220
  public Object parseTupleData(ByteBuffer buffer) throws UnsupportedEncodingException {

    HashMap<String, Object> data = new HashMap<String, Object>();
    Object result = data;

    String values = "";

    short columns = buffer.getShort(); /* (Int16) Number of columns. */

    for (int i = 0; i < columns; i++) {

      char statusValue = (char) buffer.get(); /*
       * (Byte1) Either identifies the data as NULL value ('n') or unchanged TOASTed value ('u') or text formatted value ('t').
       */

      if (i > 0) values += ",";

      if (statusValue == 't') {

        int lenValue = buffer.getInt(); /* (Int32) Length of the column value. */

        buffer.position();
        byte[] bytes = new byte[lenValue];
        buffer.get(bytes);

        values += new String(bytes, "UTF-8"); /* (ByteN) The value of the column, in text format. */

      } else {
        /* statusValue = 'n' (NULL value) or 'u' (unchanged TOASTED value) */

        values = (statusValue == 'n') ? values + "null" : values + "UTOAST";
      }
    }

    data.put("numColumns", columns);
    data.put("values", "(" + values + ")");

    result = data;

    return result;
  }

  private String readStringFromBuffer(ByteBuffer buffer) {
    StringBuilder sb = new StringBuilder();
    byte b = 0;
    while ((b = buffer.get()) != 0) {
      sb.append((char) b);
    }
    return sb.toString();
  }

  public enum MessageType {
    RELATION,
    BEGIN,
    COMMIT,
    INSERT,
    UPDATE,
    DELETE,
    TYPE,
    ORIGIN,
    TRUNCATE,
    LOGICAL_DECODING_MESSAGE;

    public static MessageType forType(char type) {
      switch (type) {
        case 'R':
          return RELATION;
        case 'B':
          return BEGIN;
        case 'C':
          return COMMIT;
        case 'I':
          return INSERT;
        case 'U':
          return UPDATE;
        case 'D':
          return DELETE;
        case 'Y':
          return TYPE;
        case 'O':
          return ORIGIN;
        case 'T':
          return TRUNCATE;
        case 'M':
          return LOGICAL_DECODING_MESSAGE;
        default:
          throw new IllegalArgumentException("Unsupported message type: " + type);
      }
    }
  }
}
