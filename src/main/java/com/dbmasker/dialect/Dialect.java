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


    /**
     * Generates an SQL UPDATE statement for the specified table with the provided conditions.
     *
     * @param connection The active database connection.
     * @param dbType The type of the database (e.g., "MySQL", "PostgreSQL").
     * @param schemaName The name of the schema.
     * @param tableName The name of the table for which the SQL update statement is to be generated.
     * @param setData The key-value pairs representing columns and their corresponding values to be updated.
     * @param condition The conditions that determine which rows will be updated.
     * @param filteredByUniqueKey Flag indicating whether to filter the conditions by unique keys.
     * @return A string representing the SQL UPDATE statement.
     * @throws SQLException If any SQL-related error occurs.
     */
    String generateUpdateSql(Connection connection, String dbType, String schemaName, String tableName,
                                    Map<String, Object> setData,
                                    Map<String, Object> condition,
                                    boolean filteredByUniqueKey) throws SQLException;

    /**
     * Generates an SQL UPDATE statement based on provided schema, table, data sets, conditions, and column types.
     *
     * @param schemaName  The name of the database schema.
     * @param tableName   The name of the table within the schema.
     * @param setData     A map containing the columns and values that need to be updated.
     * @param condition   A map containing the conditions for which rows should be updated.
     * @param columnTypes A map specifying the data type of each column.
     * @return An SQL UPDATE statement in string format.
     */
    String generateUpdateSql(String schemaName, String tableName, Map<String, Object> setData, Map<String, Object> condition, Map<String, String> columnTypes);

    /**
     * Generates a SQL DELETE statement for the given table and conditions.
     *
     * @param connection          The database connection.
     * @param dbType              The type of the database.
     * @param schemaName          The name of the schema.
     * @param tableName           The name of the table.
     * @param condition           A map representing the conditions for the DELETE operation.
     * @param filteredByUniqueKey If true, the conditions are filtered by the unique keys.
     * @return The SQL DELETE statement as a string.
     * @throws SQLException If any SQL related error occurs.
     */
    String generateDeleteSql(Connection connection, String dbType, String schemaName, String tableName,
                                    Map<String, Object> condition, boolean filteredByUniqueKey) throws SQLException;
}