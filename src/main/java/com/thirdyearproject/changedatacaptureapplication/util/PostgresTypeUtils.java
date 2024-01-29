package com.thirdyearproject.changedatacaptureapplication.util;
import org.postgresql.core.Oid;

import java.sql.Types;

public class PostgresTypeUtils {
    public static int convertOIDToJDBCType(int oid) {
        switch (oid) {
            case Oid.INT2:
                return Types.SMALLINT;
            case Oid.INT4:
                return Types.INTEGER;
            case Oid.OID:
                return Types.BIGINT;
            case Oid.INT8:
                return Types.BIGINT;
            case Oid.FLOAT4:
                return Types.REAL;
            case Oid.FLOAT8:
                return Types.DOUBLE;
            case Oid.CHAR:
                return Types.CHAR;
            case Oid.BPCHAR:
                return Types.CHAR;
            case Oid.VARCHAR:
                return Types.VARCHAR;
            case Oid.TEXT:
                return Types.VARCHAR;
            case Oid.NAME:
                return Types.VARCHAR;
            case Oid.BOOL:
                return Types.BIT;
            case Oid.BIT:
                return Types.BIT;
            default:
                throw new IllegalArgumentException("Unsupported OID: " + oid);
        }
    }
}
