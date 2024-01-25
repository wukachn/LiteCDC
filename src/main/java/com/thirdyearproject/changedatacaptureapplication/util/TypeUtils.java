package com.thirdyearproject.changedatacaptureapplication.util;

import java.sql.Types;

public class TypeUtils {
  public static String convertSqlTypeToString(int type, int size) {
    switch (type) {
      case Types.BOOLEAN -> {
        return "BOOLEAN";
      }
      case Types.TINYINT -> {
        return String.format("TINYINT(%s)", size);
      }
      case Types.SMALLINT -> {
        return String.format("SMALLINT(%s)", size);
      }
      case Types.INTEGER -> {
        return String.format("INT(%s)", size);
      }
      case Types.BIT -> {
        return String.format("BIT(%s)", size);
      }
      case Types.BIGINT -> {
        return String.format("BIGINT(%s)", size);
      }
      case Types.FLOAT -> {
        return String.format("FLOAT(%s)", size);
      }
      case Types.DOUBLE -> {
        return String.format("DOUBLE(%s)", size);
      }
      default -> {
        return String.format("VARCHAR(%s)", size);
      }
    }
  }
}
