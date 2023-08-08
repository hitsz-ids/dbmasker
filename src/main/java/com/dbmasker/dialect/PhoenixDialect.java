package com.dbmasker.dialect;

import java.util.Map;

/**
 * PhoenixDialect class implements the Dialect interface for Phoenix.
 */
public class PhoenixDialect extends BaseDialect {

    /**
     * Default constructor for PhoenixDialect class.
     */
    public PhoenixDialect() {
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
            case "INTEGER", "UNSIGNED_INT", "BIGINT", "UNSIGNED_LONG", "TINYINT", "UNSIGNED_TINYINT", "SMALLINT",
                    "UNSIGNED_SMALLINT", "FLOAT", "UNSIGNED_FLOAT", "DOUBLE", "UNSIGNED_DOUBLE", "DECIMAL" -> {
                return String.valueOf(data);
            }
            case "BOOLEAN" -> {
                if (data instanceof Boolean bool) {
                    return Boolean.TRUE.equals(bool) ? "TRUE" : "FALSE";
                } else if (data instanceof String) {
                    return data.toString().equalsIgnoreCase("TRUE") ? "TRUE" : "FALSE";
                }
            }
            case "DATE" -> {
                return "TO_DATE('" + data.toString() + "', 'yyyy-MM-dd')";
            }
            case "TIME" -> {
                return "TO_TIME('" + data.toString() + "', 'HH:mm:ss')";
            }
            case "TIMESTAMP" -> {
                return "TO_TIMESTAMP('" + data.toString() + "', 'yyyy-MM-dd HH:mm:ss')";
            }
            case "VARCHAR", "CHAR", "VARBINARY", "BINARY" -> {
                return "'" + ((String) data).replace("'", "''") + "'";
            }
            default -> {
                return data.toString();
            }
        }
        throw new IllegalArgumentException("Unsupported data type: " + type);
    }

    /**
     * This method initializes a StringBuilder for SQL generation, starting with "INSERT INTO"
     * and optionally prepending a schema name. If the data provided is empty,
     * it throws an IllegalArgumentException.
     *
     * @param schemaName The name of the schema. This is optional and can be null or empty.
     * @param data The data to be inserted. This is used to check if the data provided is empty.
     * @return A StringBuilder initialized with the "INSERT INTO" SQL command and the schema name if provided.
     * @throws IllegalArgumentException if the data provided is empty.
     */
    @Override
    protected StringBuilder sqlInit(String schemaName, Map<String, Object> data) {
        if (data.isEmpty()) {
            throw new IllegalArgumentException("No data provided to generate SQL");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("UPSERT INTO ");
        if (schemaName != null && !schemaName.trim().isEmpty()) {
            sql.append(schemaName).append(".");
        }
        return sql;
    }
}
