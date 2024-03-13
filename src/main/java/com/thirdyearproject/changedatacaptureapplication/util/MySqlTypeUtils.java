package com.thirdyearproject.changedatacaptureapplication.util;

import java.sql.Types;

public class MySqlTypeUtils {
  public static String convertNullableBooleanToString(boolean isNullable) {
    if (isNullable) {
      return "NULL";
    } else {
      return "NOT NULL";
    }
  }

  public static String convertSqlTypeToString(int type, int size) {
    switch (type) {
      case Types.BOOLEAN, Types.BIT -> {
        // MySQL represents all booleans as BIT or TINYINT(1)
        return "BIT";
      }
      case Types.TINYINT -> {
        return "TINYINT";
      }
      case Types.SMALLINT -> {
        return "SMALLINT";
      }
      case Types.INTEGER -> {
        return "INT";
      }
      case Types.BIGINT -> {
        return "BIGINT";
      }
      case Types.FLOAT, Types.REAL -> {
        return "FLOAT";
      }
      case Types.DOUBLE -> {
        return "DOUBLE";
      }
      default -> {
        if (size > 16383) {
          return "TEXT";
        }
        return String.format("VARCHAR(%s)", size);
      }
    }
  }
}
