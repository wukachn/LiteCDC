// This file is heavily inspired by debezium
// https://github.com/debezium/debezium/blob/ef69dbe91c482d4ea505368b3a1a55c40eeb5ffb/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/pgoutput/PgOutputMessageDecoder.java#L722
package com.thirdyearproject.changedatacaptureapplication.engine;


import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PgOutputMessageDecoder {

  private static final Instant PG_EPOCH =
      LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

  public PgOutputMessageDecoder() {}

  public static void processNotEmptyMessage(ByteBuffer buffer)
      throws SQLException, InterruptedException {
    final MessageType messageType = MessageType.forType((char) buffer.get());
    switch (messageType) {
      case BEGIN:
        handleBeginMessage(buffer);
        break;
      case COMMIT:
        handleCommitMessage(buffer);
        break;
      case INSERT:
        decodeInsert(buffer);
        break;
      case UPDATE:
        decodeUpdate(buffer);
        break;
      case DELETE:
        decodeDelete(buffer);
        break;
      default:
        log.info("Message Type {} skipped, not processed.", messageType);
        break;
    }
  }

  /**
   * Callback handler for the 'B' begin replication message.
   *
   * @param buffer The replication stream buffer
   */
  private static void handleBeginMessage(ByteBuffer buffer) {
    final var lsn = buffer.getLong(); // LSN
    var commitTimestamp = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
    var transactionId = Integer.toUnsignedLong(buffer.getInt());
    log.info("Event: {}", MessageType.BEGIN);
    log.info("Final LSN of transaction: {}", lsn);
    log.info("Commit timestamp of transaction: {}", commitTimestamp);
    log.info("XID of transaction: {}", transactionId);
  }

  /**
   * Callback handler for the 'C' commit replication message.
   *
   * @param buffer The replication stream buffer
   */
  private static void handleCommitMessage(ByteBuffer buffer) {
    int flags = buffer.get(); // flags, currently unused
    final var lsn = buffer.getLong(); // LSN of the commit
    final var endLsn = buffer.getLong(); // End LSN of the transaction
    Instant commitTimestamp = PG_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
    log.info("Event: {}", MessageType.COMMIT);
    log.info("Flags: {} (currently unused and most likely 0)", flags);
    log.info("Commit LSN: {}", lsn);
    log.info("End LSN of transaction: {}", endLsn);
    log.info("Commit timestamp of transaction: {}", commitTimestamp);
  }

  /**
   * Callback handler for the 'I' insert replication stream message.
   *
   * @param buffer The replication stream buffer
   */
  private static void decodeInsert(ByteBuffer buffer) {
    int relationId = buffer.getInt();
    char tupleType = (char) buffer.get(); // Always 'N" for inserts

    log.info(
        "Event: {}, Relation Id: {}, Tuple Type: {}", MessageType.INSERT, relationId, tupleType);
  }

  /**
   * Callback handler for the 'U' update replication stream message.
   *
   * @param buffer The replication stream buffer
   */
  private static void decodeUpdate(ByteBuffer buffer) {
    int relationId = buffer.getInt();

    log.info("Event: {}, RelationId: {}", MessageType.UPDATE, relationId);
  }

  /**
   * Callback handler for the 'D' delete replication stream message.
   *
   * @param buffer The replication stream buffer
   */
  private static void decodeDelete(ByteBuffer buffer) {
    int relationId = buffer.getInt();

    char tupleType = (char) buffer.get();

    log.info(
        "Event: {}, RelationId: {}, Tuple Type: {}", MessageType.DELETE, relationId, tupleType);
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
