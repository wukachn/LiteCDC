// This file is heavily inspired by debezium
// https://github.com/debezium/debezium/blob/ef69dbe91c482d4ea505368b3a1a55c40eeb5ffb/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/pgoutput/PgOutputMessageDecoder.java#L722
package com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming;

import com.thirdyearproject.changedatacaptureapplication.engine.JdbcConnection;
import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.CRUD;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ChangeEvent;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnDetails;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.ColumnWithData;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.PostgresMetadata;
import com.thirdyearproject.changedatacaptureapplication.engine.change.model.TableIdentifier;
import com.thirdyearproject.changedatacaptureapplication.util.PostgresTypeUtils;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.ResultSetMetaData;
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
  private static final String NULL_IDENTIFIER = "*NULL*";
  private final PostgresTransactionProcessor transactionProcessor;
  private JdbcConnection jdbcConnection;
  private Long currentTxId;
  private Long transactionCommitTime;

  public PgOutputMessageDecoder(
      JdbcConnection jdbcConnection, ChangeEventProducer changeEventProducer) {
    this.jdbcConnection = jdbcConnection;
    this.transactionProcessor = new PostgresTransactionProcessor(changeEventProducer);
  }

  public void processNotEmptyMessage(ByteBuffer buffer, LogSequenceNumber lsn) throws SQLException {
    final PostgresMessageType messageType = PostgresMessageType.forType((char) buffer.get());
    switch (messageType) {
      case BEGIN -> handleBeginMessage(buffer);
      case RELATION -> handleRelationMessage(buffer);
      case INSERT -> transactionProcessor.process(decodeInsert(buffer, lsn));
      case UPDATE -> transactionProcessor.process(decodeUpdate(buffer, lsn));
      case DELETE -> transactionProcessor.process(decodeDelete(buffer, lsn));
      case COMMIT -> transactionProcessor.commit(handleCommitMessage(buffer));
      default -> {}
    }
  }

  private LogSequenceNumber handleCommitMessage(ByteBuffer buffer) throws SQLException {
    int flags = buffer.get(); // flags, currently unused
    var lsn = buffer.getLong(); // LSN of the commit
    var commitLsn = buffer.getLong();
    return LogSequenceNumber.valueOf(commitLsn);
  }

  private void handleBeginMessage(ByteBuffer buffer) throws SQLException {
    var lsn = buffer.getLong();
    this.transactionCommitTime = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS).toEpochMilli();
    this.currentTxId = Integer.toUnsignedLong(buffer.getInt());
  }

  private void handleRelationMessage(ByteBuffer buffer) throws SQLException {
    int relationId = buffer.getInt();
    var schemaName = readStringFromBuffer(buffer);
    var tableName = readStringFromBuffer(buffer);
    var tableId = TableIdentifier.of(schemaName, tableName);

    int replicaIdentityId = buffer.get();
    short columnCount = buffer.getShort();

    List<String> primaryKeys = new ArrayList<>();
    Map<String, PgOutputColumnMetadata> columnMetadata = new HashMap<>();
    try (var conn = jdbcConnection.getConnection()) {
      var metadata = conn.getMetaData();

      var rsPk = metadata.getPrimaryKeys(null, schemaName, tableName);
      while (rsPk.next()) {
        var columnName = rsPk.getString(4);
        primaryKeys.add(columnName);
      }

      var rsCol = metadata.getColumns(null, schemaName, tableName, null);
      while (rsCol.next()) {
        var columnName = rsCol.getString(4);
        var jdbcNullable = rsCol.getInt(11);
        var isNullable =
            jdbcNullable == ResultSetMetaData.columnNullable
                || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
        var size = rsCol.getInt(7);

        columnMetadata.put(
            columnName, PgOutputColumnMetadata.builder().isNullable(isNullable).size(size).build());
      }
    }

    List<ColumnDetails> columns = new ArrayList<>();
    for (var i = 0; i < columnCount; i++) {
      byte flags = buffer.get();
      var name = readStringFromBuffer(buffer);
      var oid = buffer.getInt();
      int attypmod = buffer.getInt();

      var type = PostgresTypeUtils.convertOIDToJDBCType(oid);

      var metadata = columnMetadata.get(name);
      var size = metadata.getSize();
      var isNullable = metadata.isNullable();
      var isPrimaryKey = primaryKeys.contains(name);

      columns.add(
          ColumnDetails.builder()
              .name(name)
              .sqlType(type)
              .size(size)
              .isNullable(isNullable)
              .isPrimaryKey(isPrimaryKey)
              .build());
    }

    tableColumnMap.put(relationId, columns);
    relationIdToTableId.put(relationId, tableId);
  }

  private ChangeEvent decodeInsert(ByteBuffer buffer, LogSequenceNumber lsn) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get();

    var tableId = relationIdToTableId.get(relationId);

    var metadata =
        PostgresMetadata.builder()
            .tableId(tableId)
            .op(CRUD.CREATE)
            .lsn(lsn)
            .txId(currentTxId)
            .dbCommitTime(transactionCommitTime)
            .build();

    var after = buildColumnData(relationId, buffer);

    var changeEvent = ChangeEvent.builder().metadata(metadata).before(null).after(after).build();
    return changeEvent;
  }

  private ChangeEvent decodeUpdate(ByteBuffer buffer, LogSequenceNumber lsn) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get(); // N or (O or K)

    var tableId = relationIdToTableId.get(relationId);

    var metadata =
        PostgresMetadata.builder()
            .tableId(tableId)
            .op(CRUD.UPDATE)
            .lsn(lsn)
            .txId(currentTxId)
            .dbCommitTime(transactionCommitTime)
            .build();

    List<ColumnWithData> before;
    // Read before values if available. Otherwise, leave before empty.
    if ('O' == tupleType || 'K' == tupleType) {
      before = buildColumnData(relationId, buffer);

      // Increment position
      tupleType = (char) buffer.get();
    } else {
      before = null;
    }

    var after = buildColumnData(relationId, buffer);

    var changeEvent = ChangeEvent.builder().metadata(metadata).before(before).after(after).build();

    return changeEvent;
  }

  private ChangeEvent decodeDelete(ByteBuffer buffer, LogSequenceNumber lsn) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get();

    var tableId = relationIdToTableId.get(relationId);

    var metadata =
        PostgresMetadata.builder()
            .tableId(tableId)
            .op(CRUD.DELETE)
            .lsn(lsn)
            .txId(currentTxId)
            .dbCommitTime(transactionCommitTime)
            .build();

    var before = buildColumnData(relationId, buffer);

    var changeEvent = ChangeEvent.builder().metadata(metadata).before(before).after(null).build();

    return changeEvent;
  }

  private List<ColumnWithData> buildColumnData(Integer relationId, ByteBuffer buffer) {
    try {
      Map<String, Object> tupleMap = (Map<String, Object>) parseTupleData(buffer);

      var values = (String) tupleMap.get("values");
      var csv = values.substring(1, values.length() - 1);
      String[] valuesArray = csv.split(",");

      var columnList = new ArrayList<ColumnWithData>();

      var columnDetailsList = tableColumnMap.get(relationId);
      var i = 0;
      for (var columnDetails : columnDetailsList) {
        var type = columnDetails.getSqlType();
        Object value = null;
        if (!valuesArray[i].equals(NULL_IDENTIFIER)) {
          switch (type) {
            case Types.BOOLEAN -> value = Boolean.valueOf(valuesArray[i]);
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIT -> value = Integer.valueOf(valuesArray[i]);
            case Types.BIGINT -> value = Long.valueOf(valuesArray[i]);
            case Types.FLOAT -> value = Float.valueOf(valuesArray[i]);
            case Types.DOUBLE -> value = Double.valueOf(valuesArray[i]);
            default -> value = String.valueOf(valuesArray[i]);
          }
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

        values = (statusValue == 'n') ? values + NULL_IDENTIFIER : values + "UTOAST";
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
}
