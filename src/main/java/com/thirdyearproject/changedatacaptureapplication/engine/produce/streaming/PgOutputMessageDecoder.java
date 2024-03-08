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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

/*
This file is heavily inspired by the following repositories:
https://github.com/debezium/debezium/blob/ef69dbe91c482d4ea505368b3a1a55c40eeb5ffb/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/pgoutput/PgOutputMessageDecoder.java#L722
https://github.com/davyam/pgEasyReplication/blob/master/src/main/java/com/dg/easyReplication/Decode.java#
 */
@Slf4j
public class PgOutputMessageDecoder {

  private static final Instant PG_EPOCH =
      LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);
  private static final Map<Integer, List<ColumnDetails>> tableColumnMap = new HashMap<>();
  private static final Map<Integer, TableIdentifier> relationIdToTableId = new HashMap<>();
  private static final String NULL_IDENTIFIER = "*NULL*";
  private final PostgresTransactionProcessor transactionProcessor;
  private final JdbcConnection jdbcConnection;
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

  private LogSequenceNumber handleCommitMessage(ByteBuffer buffer) {
    int flags = buffer.get(); // currently unused
    var lsn = buffer.getLong();
    var commitLsn = buffer.getLong();
    return LogSequenceNumber.valueOf(commitLsn);
  }

  private void handleBeginMessage(ByteBuffer buffer) {
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

    return ChangeEvent.builder().metadata(metadata).before(null).after(after).build();
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

    return ChangeEvent.builder().metadata(metadata).before(before).after(after).build();
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

    return ChangeEvent.builder().metadata(metadata).before(before).after(null).build();
  }

  private List<ColumnWithData> buildColumnData(Integer relationId, ByteBuffer buffer) {
    var columnList = new ArrayList<ColumnWithData>();
    var values = getColumnValues(buffer);
    var columnDetailsList = tableColumnMap.get(relationId);
    var i = 0;
    for (var columnDetails : columnDetailsList) {
      var type = columnDetails.getSqlType();
      Object value = null;
      if (!values.get(i).equals(NULL_IDENTIFIER)) {
        switch (type) {
          case Types.BOOLEAN -> value = Boolean.valueOf(values.get(i));
          case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIT -> value =
              Integer.valueOf(values.get(i));
          case Types.BIGINT -> value = Long.valueOf(values.get(i));
          case Types.FLOAT -> value = Float.valueOf(values.get(i));
          case Types.DOUBLE -> value = Double.valueOf(values.get(i));
          default -> value = String.valueOf(values.get(i));
        }
      }
      columnList.add(ColumnWithData.builder().details(columnDetails).value(value).build());
      i++;
    }
    return columnList;
  }

  public List<String> getColumnValues(ByteBuffer buffer) {
    List<String> values = new ArrayList<>();

    short numColumns = buffer.getShort();
    for (int i = 0; i < numColumns; i++) {
      // Identifies the data as NULL value ('n') or unchanged TOASTed value ('u') or text formatted
      // value ('t').
      char statusValue = (char) buffer.get();

      // Add text formatted value to values list.
      if (statusValue == 't') {
        int columnValueLength = buffer.getInt();
        byte[] bytes = new byte[columnValueLength];
        buffer.get(bytes);
        values.add(new String(bytes, StandardCharsets.UTF_8));
      } else {
        // Use NULL_IDENTIFIER to eventually convert into real null value.
        values.add((statusValue == 'n') ? NULL_IDENTIFIER : "UTOAST");
      }
    }
    return values;
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
