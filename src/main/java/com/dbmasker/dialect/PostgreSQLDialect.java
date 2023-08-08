package com.dbmasker.dialect;


import com.dbmasker.utils.DbUtils;


/**
 * PostgreSQLDialect class implements the Dialect interface for PostgreSQL.
 */
public class PostgreSQLDialect extends BaseDialect {

    /**
     * Default constructor for PostgreSQLDialect class.
     */
    public PostgreSQLDialect() {
        super();
    }

    /**
     * Formats data into a PostgreSQL-friendly string for SQL statements, based on the specified type.
     *
     * @param data The object to be formatted, could be null.
     * @param type The String representing the PostgreSQL data type, such as "CHARACTER", "VARCHAR", "BPCHAR", "TEXT", etc.
     * @return The String representation of the data formatted for PostgreSQL SQL statement.
     * If the input data is null, the function will return "NULL".
     * <p>
     * Note:
     * - For a BOOLEAN type, the function converts the Boolean object to a String.
     * - For a String type that includes "CHARACTER", "CHAR", "CHARACTER VARYING", "VARCHAR", "BPCHAR", or "TEXT",
     *   the function replaces single quotes in the String with two single quotes for SQL compatibility and then wraps the String in single quotes.
     * - For a type that includes "TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ", or "INTERVAL",
     *   the function converts the object to a String, replaces single quotes in the String with two single quotes, and wraps the String in single quotes.
     * - For all other types, the function simply calls the toString() method on the object.
     */
    @Override
    public String formatData(Object data, String type) {
        if (data == null) {
            return "NULL";
        }
        if (type == null) {
            return data.toString();
        }

        String typeUpper = type.toUpperCase();
        if (typeUpper.equals("NULL")) {
            return "NULL";
        }
        if (typeUpper.equals("BOOLEAN")) {
            if (data instanceof Boolean boolData) {
                return Boolean.toString(boolData);
            } else {
                return data.toString();
            }
        }
        if (typeUpper.startsWith("CHARACTER") || typeUpper.startsWith("CHAR") || typeUpper.startsWith("VARCHAR")
                || typeUpper.startsWith("BPCHAR") || typeUpper.equals("TEXT")) {
            String text = data.toString();
            text = text.replace("'", "''");  // Replace single quotes with two single quotes.
            return "'" + text + "'";
        }
        if (typeUpper.startsWith("TIME") || typeUpper.startsWith("TIMETZ")
                || typeUpper.startsWith("TIMESTAMP") || typeUpper.startsWith("TIMESTAMPTZ")
                || typeUpper.startsWith("INTERVAL")) {
            String text = data.toString();
            text = text.replace("'", "''");  // Replace single quotes with two single quotes.
            return "'" + text + "'";
        }
        if (typeUpper.equals("BYTEA")) {
            String byteaData = DbUtils.handBinaryData(data);
            return "E'\\\\x" + byteaData + "'";  // E'\\x...' is PostgreSQL's syntax for bytea literals
        }
        return data.toString();  // For other types, use the default integer handling
    }

}
