package com.dbmasker.dialect.gbase;


import com.dbmasker.dialect.BaseDialect;
import com.dbmasker.utils.DbUtils;

/**
 * Gbase8aDialect class implements the Dialect interface for Gbase 8a Database.
 */
public class Gbase8aDialect extends BaseDialect {

    /**
     * Default constructor for Gbase8aDialect class.
     */
    public Gbase8aDialect() {
        super();
    }

    /**
     * This method formats Java objects to be ready to use in SQL queries based on their SQL datatype.
     *
     * @param data The Java object data to be formatted.
     * @param type The SQL datatype of the column where the data will be used.
     * @return A string that is ready to be used in an SQL query.
     */
    @Override
    public String formatData(Object data, String type) {
        if (data == null) {
            return "NULL";
        }
        if (type == null) {
            return data.toString();
        }

        switch (type.toUpperCase()) {
            case "SMALLINT", "INT", "BIGINT", "REAL", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC" -> {
                return String.valueOf(data);
            }
            case "CHAR", "VARCHAR", "TEXT" -> {
                return "'" + data.toString().replace("'", "''") + "'";
            }
            case "DATE", "TIME", "TIMESTAMP" -> {
                return DbUtils.handDateData(data);
            }
            case "BOOLEAN" -> {
                if (data instanceof Boolean) {
                    return (boolean) data ? "1" : "0";
                } else {
                    return data.toString();
                }
            }
            case "BINARY", "BLOB", "LONGBLOB" -> {
               return "X'" + DbUtils.handBinaryData(data) + "'";
            }
            default -> {
                return data.toString();
            }
        }
    }
}
