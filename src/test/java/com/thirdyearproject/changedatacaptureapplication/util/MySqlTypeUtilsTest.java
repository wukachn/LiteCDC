package com.thirdyearproject.changedatacaptureapplication.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.Test;

public class MySqlTypeUtilsTest {

  @Test
  public void convertNullableBooleanToString_Nullable() {
    assertTrue(MySqlTypeUtils.convertNullableBooleanToString(true).equals("NULL"));
  }

  @Test
  public void convertNullableBooleanToString_NotNullable() {
    assertTrue(MySqlTypeUtils.convertNullableBooleanToString(false).equals("NOT NULL"));
  }

  @Test
  public void convertSqlTypeToString_Boolean() {
    assertTrue(MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.BOOLEAN, 0).equals("BIT"));
  }

  @Test
  public void convertSqlTypeToString_TinyInt() {
    assertTrue(MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.TINYINT, 5).equals("TINYINT"));
  }

  @Test
  public void convertSqlTypeToString_SmallInt() {
    assertTrue(
        MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.SMALLINT, 10).equals("SMALLINT"));
  }

  @Test
  public void convertSqlTypeToString_Integer() {
    assertTrue(MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.INTEGER, 15).equals("INT"));
  }

  @Test
  public void convertSqlTypeToString_Bit() {
    assertTrue(MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.BIT, 20).equals("BIT"));
  }

  @Test
  public void convertSqlTypeToString_BigInt() {
    assertTrue(MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.BIGINT, 25).equals("BIGINT"));
  }

  @Test
  public void convertSqlTypeToString_Float() {
    assertTrue(MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.FLOAT, 30).equals("FLOAT"));
  }

  @Test
  public void convertSqlTypeToString_Double() {
    assertTrue(MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.DOUBLE, 35).equals("DOUBLE"));
  }

  @Test
  public void convertSqlTypeToString_Text() {
    assertTrue(MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.VARCHAR, 17000).equals("TEXT"));
  }

  @Test
  public void convertSqlTypeToString_Default() {
    assertTrue(
        MySqlTypeUtils.convertSqlTypeToString(java.sql.Types.OTHER, 100).equals("VARCHAR(100)"));
  }
}
