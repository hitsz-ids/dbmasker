package com.dbmasker.api;

import com.dbmasker.dialect.Dialect;
import com.dbmasker.dialect.DialectFactory;
import com.dbmasker.utils.ErrorMessages;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * DBDialectManager is the main class for the DBMasker's dialect API.
 * This class is used to handle SQL statements for different database dialects.
 */
public class DBDialectManager {

    /**
     * Default constructor for DBDialectManager class.
     */
    private DBDialectManager() {
        throw new UnsupportedOperationException();
    }

    /**
     * Generates a SQL INSERT statement for the given data.
     *
     * @param connection The connection to the database.
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param schemaName  The name of the schema where the table is located. If null or empty, the schema name is not included in the SQL.
     * @param tableName   The name of the table where the data should be inserted.
     * @param data        A map containing the data to be inserted. Each entry in the map represents a column and its corresponding value.
     * @return A string representing the SQL INSERT statement.
     * @throws SQLException If any SQL-related error occurs.
     */
    public static String generateInsertSql(Connection connection, String dbType, String schemaName,
                                            String tableName, Map<String, Object> data) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (tableName == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Dialect dialect = new DialectFactory().getDialect(dbType);
        return dialect.generateInsertSql(connection, schemaName, tableName, data, dbType);
    }

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
    public static String generateUpdateSql(Connection connection, String dbType, String schemaName,
                                           String tableName, Map<String, Object> setData, Map<String, Object> condition,
                                           boolean filteredByUniqueKey) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (tableName == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Dialect dialect = new DialectFactory().getDialect(dbType);
        return dialect.generateUpdateSql(connection, dbType, schemaName, tableName, setData, condition, filteredByUniqueKey);
    }

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
    public static String generateDeleteSql(Connection connection, String dbType, String schemaName, String tableName,
                                           Map<String, Object> condition, boolean filteredByUniqueKey)  throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (tableName == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Dialect dialect = new DialectFactory().getDialect(dbType);
        return dialect.generateDeleteSql(connection, dbType, schemaName, tableName, condition, filteredByUniqueKey);
    }
}
