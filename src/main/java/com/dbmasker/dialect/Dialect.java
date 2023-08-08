package com.dbmasker.dialect;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;


/**
 * This interface defines the methods that a dialect must implement.
 */
public interface Dialect {

    /**
     * Formats the provided data object into a string suitable for a sql query,
     * based on the provided data type.
     *
     * @param data The data object that needs to be formatted.
     * @param type The data type of the data object (e.g., "NULL", "INTEGER", "REAL", "TEXT", "BLOB").
     * @return A string that represents the data object in a format suitable for a sql query.
     * @throws IllegalArgumentException If the provided data type is unknown.
     */
    String formatData(Object data, String type);

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
    String generateInsertSql(Connection connection, String schemaName, String tableName, Map<String, Object> data, String dbType) throws SQLException;

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
    String generateInsertSql(String schemaName, String tableName, Map<String, Object> data, Map<String, String> columnTypes);
}