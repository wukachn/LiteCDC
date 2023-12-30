// This file is heavily inspired by debezium
// https://github.com/debezium/debezium/blob/ef69dbe91c482d4ea505368b3a1a55c40eeb5ffb/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/pgoutput/PgOutputMessageDecoder.java#L722
package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.engine.change.model.CRUD;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnWithData;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.PostgresMetadata;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.replication.LogSequenceNumber;

@Slf4j
public class PgOutputMessageDecoder {

  private static final Instant PG_EPOCH =
      LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
  private static final Map<Integer, List<ColumnDetails>> tableColumnMap = new HashMap();
  private static final Map<Integer, TableIdentifier> relationIdToTableId = new HashMap();
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

    tableColumnMap.put(relationId, columns);
    relationIdToTableId.put(relationId, tableId);
  }

  private Optional<ChangeEvent> decodeInsert(ByteBuffer buffer, LogSequenceNumber lsn) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get();

    var tableId = relationIdToTableId.get(relationId);

    var metadata = PostgresMetadata.builder().tableId(tableId).op(CRUD.CREATE).lsn(lsn).build();

    var after = buildColumnData(tableId, buffer);

    var changeEvent = ChangeEvent.builder().metadata(metadata).before(null).after(after).build();
    return Optional.of(changeEvent);
  }

  private Optional<ChangeEvent> decodeUpdate(ByteBuffer buffer, LogSequenceNumber lsn) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get(); // N or (O or K)

    var tableId = relationIdToTableId.get(relationId);

    var metadata = PostgresMetadata.builder().tableId(tableId).op(CRUD.UPDATE).lsn(lsn).build();

    List<ColumnWithData> before;
    // Read before values if available. Otherwise, leave before empty.
    if ('O' == tupleType || 'K' == tupleType) {
      before = buildColumnData(tableId, buffer);

      // Increment position
      tupleType = (char) buffer.get();
    } else {
      before = null;
    }

    var after = buildColumnData(tableId, buffer);

    var changeEvent = ChangeEvent.builder().metadata(metadata).before(before).after(after).build();

    return Optional.of(changeEvent);
  }

  private Optional<ChangeEvent> decodeDelete(ByteBuffer buffer, LogSequenceNumber lsn) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get();

    var tableId = relationIdToTableId.get(relationId);

    var metadata = PostgresMetadata.builder().tableId(tableId).op(CRUD.DELETE).lsn(lsn).build();

    var before = buildColumnData(tableId, buffer);

    var changeEvent = ChangeEvent.builder().metadata(metadata).before(null).after(null).build();

    return Optional.of(changeEvent);
  }

  private List<ColumnWithData> buildColumnData(TableIdentifier tableId, ByteBuffer buffer) {
    try {
      Map<String, Object> tupleMap = (Map<String, Object>) parseTupleData(buffer);

      var values = (String) tupleMap.get("values");
      var csv = values.substring(1, values.length() - 1);
      String[] valuesArray = csv.split(",");

      var columnList = new ArrayList<ColumnWithData>();

      var columnDetailsList = tableColumnMap.get(tableId);
      var i = 0;
      for (var columnDetails : columnDetailsList) {
        var type = columnDetails.getSqlType();
        Object value;
        switch (type) {
          case Types.BOOLEAN -> value = Boolean.valueOf(valuesArray[i]);
          case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIT -> value =
              Integer.valueOf(valuesArray[i]);
          case Types.BIGINT -> value = Long.valueOf(valuesArray[i]);
          case Types.FLOAT -> value = Float.valueOf(valuesArray[i]);
          case Types.DOUBLE -> value = Double.valueOf(valuesArray[i]);
          default -> value = String.valueOf(valuesArray[i]);
        }
        columnList.add(ColumnWithData.builder().details(columnDetails).value(value).build());
        i++;
      }
      return columnList;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Source:
  // https://github.com/davyam/pgEasyReplication/blob/master/src/main/java/com/dg/easyReplication/Decode.java#L220
  // Whole file is heavily inspired by that link and debezium.
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
