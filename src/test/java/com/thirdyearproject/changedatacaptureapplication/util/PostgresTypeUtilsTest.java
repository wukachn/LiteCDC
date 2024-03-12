package com.thirdyearproject.changedatacaptureapplication.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.Test;

public class PostgresTypeUtilsTest {

  @Test
  public void convertOIDToJDBCType_Int2() {
    assertEquals(
        java.sql.Types.SMALLINT,
        PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.INT2));
  }

  @Test
  public void convertOIDToJDBCType_Int4() {
    assertEquals(
        java.sql.Types.INTEGER,
        PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.INT4));
  }

  @Test
  public void convertOIDToJDBCType_OID() {
    assertEquals(
        java.sql.Types.BIGINT, PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.OID));
  }

  @Test
  public void convertOIDToJDBCType_Int8() {
    assertEquals(
        java.sql.Types.BIGINT,
        PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.INT8));
  }

  @Test
  public void convertOIDToJDBCType_Float4() {
    assertEquals(
        java.sql.Types.REAL,
        PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.FLOAT4));
  }

  @Test
  public void convertOIDToJDBCType_Float8() {
    assertEquals(
        java.sql.Types.DOUBLE,
        PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.FLOAT8));
  }

  @Test
  public void convertOIDToJDBCType_Char() {
    assertEquals(
        java.sql.Types.CHAR, PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.CHAR));
  }

  @Test
  public void convertOIDToJDBCType_BpChar() {
    assertEquals(
        java.sql.Types.CHAR,
        PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.BPCHAR));
  }

  @Test
  public void convertOIDToJDBCType_Varchar() {
    assertEquals(
        java.sql.Types.VARCHAR,
        PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.VARCHAR));
  }

  @Test
  public void convertOIDToJDBCType_Text() {
    assertEquals(
        java.sql.Types.VARCHAR,
        PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.TEXT));
  }

  @Test
  public void convertOIDToJDBCType_Name() {
    assertEquals(
        java.sql.Types.VARCHAR,
        PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.NAME));
  }

  @Test
  public void convertOIDToJDBCType_Bool() {
    assertEquals(
        java.sql.Types.BIT, PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.BOOL));
  }

  @Test
  public void convertOIDToJDBCType_Bit() {
    assertEquals(
        java.sql.Types.BIT, PostgresTypeUtils.convertOIDToJDBCType(org.postgresql.core.Oid.BIT));
  }

  @Test
  public void convertOIDToJDBCType_InvalidOID() {
    assertThrows(IllegalArgumentException.class, () -> PostgresTypeUtils.convertOIDToJDBCType(-1));
  }
}
