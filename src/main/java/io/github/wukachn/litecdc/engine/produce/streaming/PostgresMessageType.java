package io.github.wukachn.litecdc.engine.produce.streaming;

public enum PostgresMessageType {
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

  public static PostgresMessageType forType(char type) {
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
