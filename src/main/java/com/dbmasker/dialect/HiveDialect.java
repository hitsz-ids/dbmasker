package com.dbmasker.dialect;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * HiveDialect class implements the Dialect interface for Hive.
 */
public class HiveDialect extends BaseDialect {

    /**
     * Default constructor for HiveDialect class.
     */
    public HiveDialect() {
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

            SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

            return switch (type.toUpperCase()) {
                case "TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "FLOAT", "DOUBLE", "DECIMAL":
                    yield String.valueOf(data);
                case "STRING", "VARCHAR", "CHAR", "BINARY":
                    yield "'" + data.toString().replace("'", "''") + "'";
                case "TIMESTAMP", "DATE":
                    if (data instanceof Timestamp timestamp) {
                        yield "'" + timestampFormatter.format(timestamp) + "'";
                    } else if (data instanceof Date date) {
                        yield "'" + dateFormatter.format(date) + "'";
                    } else {
                        yield "'" + data + "'";
                    }
                default:
                    yield  data.toString();
            };
    }


    /**
     * Generates a SQL INSERT statement for the given data.
     *
     * @param schemaName  The name of the schema where the table is located. If null or empty, the schema name is not included in the SQL.
     * @param tableName   The name of the table where the data should be inserted.
     * @param data        A map containing the data to be inserted. Each entry in the map represents a column and its corresponding value.
     * @param columnTypes A map containing the data types of each column. Each entry in the map represents a column and its corresponding data type.
     * @return A string representing the SQL INSERT statement.
     * @throws IllegalArgumentException If the provided data is empty.
     */
    @Override
    public String generateInsertSql(String schemaName, String tableName, Map<String, Object> data, Map<String, String> columnTypes) {
        StringBuilder sql = sqlInit(schemaName, data);
        sql.append(tableName).append(" SELECT\n");

        // Append values
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String column = entry.getKey().toLowerCase();
            Object value = entry.getValue();
            String type = columnTypes.get(column);
            sql.append(formatData(value, type)).append(" AS ").append(column).append(",");
        }

        // Replace the last comma with a space
        sql.setCharAt(sql.length() - 1, ' ');

        sql.append("FROM (SELECT 1) t;");

        return sql.toString();
    }

}
