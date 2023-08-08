package com.dbmasker.dialect;


import com.dbmasker.database.Database;
import com.dbmasker.database.DatabaseFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * BaseDialect class implements the Dialect interface.
 */
public abstract class BaseDialect implements Dialect {

    /**
     * Default constructor for BaseDialect class.
     */
    protected BaseDialect() {
        super();
    }

    /**
     * Generates a SQL INSERT statement for the given data.
     *
     * @param connection The connection to the database.
     * @param schemaName  The name of the schema where the table is located. If null or empty, the schema name is not included in the SQL.
     * @param tableName   The name of the table where the data should be inserted.
     * @param data        A map containing the data to be inserted. Each entry in the map represents a column and its corresponding value.
     * @param dbType      The type of the database (e.g., "sqlite", "mysql", etc.).
     * @return A string representing the SQL INSERT statement.
     * @throws IllegalArgumentException If the provided data is empty.
     */
    @Override
    public String generateInsertSql(Connection connection, String schemaName, String tableName, Map<String, Object> data, String dbType) throws SQLException {
        Database database = new DatabaseFactory().getDatabase(dbType);
        Map<String, String> columnTypes = database.getColumnTypes(connection, schemaName, tableName);
        return generateInsertSql(schemaName, tableName, data, columnTypes);
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
        sql.append(tableName).append(" (");

        // Append column names
        for (String column : data.keySet()) {
            sql.append(column).append(",");
        }

        // Replace the last comma with a closing parenthesis
        sql.setCharAt(sql.length() - 1, ')');

        sql.append(" VALUES\n(");

        // Append values
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String column = entry.getKey().toLowerCase();
            Object value = entry.getValue();
            String type = columnTypes.get(column);
            sql.append(formatData(value, type)).append(",");
        }

        // Replace the last comma with a closing parenthesis
        sql.setCharAt(sql.length() - 1, ')');

        sql.append(";");

        return sql.toString();
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
    protected StringBuilder sqlInit(String schemaName, Map<String, Object> data) {
        if (data.isEmpty()) {
            throw new IllegalArgumentException("No data provided to generate SQL");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        if (schemaName != null && !schemaName.trim().isEmpty()) {
            sql.append(schemaName).append(".");
        }
        return sql;
    }
}
