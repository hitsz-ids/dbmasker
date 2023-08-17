package com.dbmasker.database;

import com.dbmasker.data.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Database interface for different database implementations.
 * Provides a method to retrieve a list of schemas from the database.
 */
public interface Database {

    /**
     * Retrieves a list of schema names from the given database connection.
     *
     * @param connection A java.sql.Connection object representing the database connection.
     * @return A list of schema names as strings.
     * @throws SQLException if a database access error occurs
     */
    List<String> getSchemas(Connection connection) throws SQLException;

    /**
     * Retrieves the names of all tables within the specified schema of a database.
     *
     * @param connection the java.sql.Connection object for the database
     * @param schemaName the name of the schema for which to retrieve table names
     * @return a list of table names within the specified schema
     * @throws SQLException if a database access error occurs
     */
    List<String> getTables(Connection connection, String schemaName) throws SQLException;

    /**
     * Retrieves the names of all views within the specified schema of a database.
     *
     * @param connection the java.sql.Connection object for the database
     * @param schemaName the name of the schema for which to retrieve view names
     * @return a list of view names within the specified schema
     * @throws SQLException if a database access error occurs
     */
    List<String> getViews(Connection connection, String schemaName) throws SQLException;

    /**
     * Retrieves a list of functions in the specified schema.
     *
     * @param connection A valid database connection.
     * @param schemaName The name of the specified schema.
     * @return Returns a list of DatabaseFunction objects, containing all the functions in the given schema.
     * @throws SQLException if a database access error occurs
     */
    List<DatabaseFunction> getFuncs(Connection connection, String schemaName) throws SQLException;

    /**
     * Retrieves metadata for tables in a given schema.
     *
     * @param connection A valid database connection.
     * @param schemaName The name of the schema for which metadata is to be retrieved.
     * @return A list of TableMetaData objects containing metadata for the tables in the given schema.
     * @throws SQLException if a database access error occurs
     */
    List<TableMetaData> getMetaData(Connection connection, String schemaName) throws SQLException;

    /**
     * Retrieves table attributes for a given table in the specified schema.
     *
     * @param connection  A valid database connection.
     * @param schemaName  The schema name where the table is located.
     * @param table       The table for which attributes are to be retrieved.
     * @return A list of TableAttribute objects containing the attributes of the given table.
     * @throws SQLException if a database access error occurs
     */
    List<TableAttribute> getTableAttribute(Connection connection, String schemaName, String table) throws SQLException;

    /**
     * Retrieves the column types of a given table in a database.
     *
     * @param connection The connection to the database.
     * @param schemaName The name of the schema where the table is located.
     * @param table      The name of the table to get column types for.
     * @return           A map where each entry represents a column in the table and its corresponding type.
     *                   The keys in the map are the column names, and the values are the corresponding data types.
     * @throws SQLException If a database access error occurs.
     */
    Map<String, String> getColumnTypes(Connection connection, String schemaName, String table) throws SQLException;

    /**
     * Retrieves the primary keys of a specified table within the given schema.
     *
     * @param connection A valid database connection.
     * @param schemaName The specified schema name.
     * @param table The specified table name.
     * @return Returns a Set of primary key column names for the specified table.
     * @throws SQLException if a database access error occurs
     */
    Set<String> getPrimaryKeys(Connection connection, String schemaName, String table) throws SQLException;

    /**
     * Retrieves the unique keys of a specified database table.
     *
     * @param connection A valid database connection.
     * @param schemaName The specified schema name.
     * @param table The specified table name.
     * @return Returns a Map, where the key is the unique key name, and the value is a collection of unique key column names.
     * @throws SQLException if a database access error occurs
     */
    Map<String, Set<String>> getUniqueKeys(Connection connection, String schemaName, String table) throws SQLException;

    /**
     * Retrieves a list of indices for the specified table in the given schema.
     * @param connection A valid database connection.
     * @param schemaName The name of the schema where the table is located.
     * @param table The name of the table for which to retrieve the indices.
     * @return A list of {@link TableIndex} objects, each containing information about an index in the specified table.
     * @throws SQLException if a database access error occurs
     */
     List<TableIndex> getIndex(Connection connection, String schemaName, String table) throws SQLException;

    /**
     * Executes the given update SQL query using the provided database connection.
     *
     * @param connection The database connection to use for executing the query.
     * @param sql        The SQL query string to execute.
     * @return rowCount  The number of rows affected by the query.
     * @throws SQLException if a database access error occurs
     */
    int executeUpdateSQL(Connection connection, String sql) throws SQLException;

    /**
     * Executes a batch of update SQL statements and returns the number of successfully updated records.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the batch update
     * @param sqlList The list of update SQL statements to be executed
     * @return rowCount The number of records successfully updated (int)
     * @throws SQLException if a database access error occurs
     */
     int executeUpdateSQLBatch(Connection connection, List<String> sqlList) throws SQLException;

    /**
     * Executes a SQL query and returns the results as a list of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param sql The SQL query string (String) to be executed
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data
     * @throws SQLException if a database access error occurs
     */
    List<Map<String, Object>> executeQuerySQL(Connection connection, String sql) throws SQLException;

    /**
     * Executes a SQL query and returns the results as a list of maps and applies obfuscation rules to the specified columns.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @param sql The SQL query string (String) to be executed
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data
     * @throws SQLException if a database access error occurs
     */
    List<Map<String, Object>> execQuerySQLWithMask(Connection connection, String sql, Map<String, ObfuscationRule> obfuscationRules) throws SQLException;

    /**
     * Executes a batch of SQL queries and returns the results as a list of lists of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlList The list of SQL query strings to be executed
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data
     * @throws SQLException if a database access error occurs
     */
     List<List<Map<String, Object>>> executeQuerySQLBatch(Connection connection, List<String> sqlList) throws SQLException;

    /**
     * Executes a SQL query or update statement, returns the results as a list of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param sql The SQL query or update statement to be executed
     * @return A list of maps where each map represents a row in the result. For a query statement, each map represents
     *         a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    List<Map<String, Object>> executeSQL(Connection connection, String sql) throws SQLException;

    /**
     * Executes a SQL query or update statement, returns the results as a list of maps.
     * Applies obfuscation rules to the specified columns.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param sql The SQL query or update statement to be executed
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    List<Map<String, Object>> executeSQL(Connection connection, String sql, Map<String, ObfuscationRule> obfuscationRules) throws SQLException;

    /**
     * Executes a batch of SQL query or update statement, and returns the results as a list of lists of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlList The list of SQL query or update statements to be executed
     * @return A list of lists of maps where each inner list represents a result. For a query statement, each map
     *         representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    List<List<Map<String, Object>>> executeSQLBatch(Connection connection, List<String> sqlList) throws SQLException;

    /**
     * Executes a batch of SQL query or update statement, and returns the results as a list of lists of maps.
     * Applies obfuscation rules to the specified columns.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlList The list of SQL query or update statements to be executed
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    List<List<Map<String, Object>>> executeSQLBatch(Connection connection, List<String> sqlList, Map<String, ObfuscationRule> obfuscationRules) throws SQLException;

    /**
     * Executes a script of SQL query or update statement, and returns the results as a list of lists of maps.
     * The script is split into individual statements using the semicolon as a delimiter.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlScript The script of SQL query or update statements to be executed, separated by semicolons
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    List<List<Map<String, Object>>> executeSQLScript(Connection connection, String sqlScript) throws SQLException;

    /**
     * Executes a script of SQL query or update statement, and returns the results as a list of lists of maps.
     * The script is split into individual statements using the semicolon as a delimiter.
     * Applies obfuscation rules to the specified columns.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlScript The script of SQL query or update statements to be executed, separated by semicolons
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    List<List<Map<String, Object>>> executeSQLScript(Connection connection, String sqlScript, Map<String, ObfuscationRule> obfuscationRules) throws SQLException;

    /**
     * Commits a transaction for the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) on which the transaction is being executed
     * @return true if the transaction was successfully committed, false otherwise
     * @throws SQLException if a database access error occurs
     */
     boolean commit(Connection connection) throws SQLException;

    /**
     * Sets the auto-commit mode for the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) for which the auto-commit mode should be set
     * @param autoCommit The auto-commit mode (boolean) to set for the connection
     * @return true if the auto-commit mode was successfully set, false otherwise
     * @throws SQLException if a database access error occurs
     */
     boolean setAutoCommit(Connection connection, boolean autoCommit) throws SQLException;

    /**
     * Rolls back a transaction for the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) on which the transaction is being executed
     * @return true if the transaction was successfully rolled back, false otherwise
     * @throws SQLException if a database access error occurs
     */
     boolean rollback(Connection connection) throws SQLException;

    /**
     * Fetches data from the specified table or view in the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) to fetch the data from
     * @param schemaName The name of the schema where the table is located.
     * @param name  The name of the table/view to fetch the data from
     * @return A List of Maps containing the table/view data, where each Map represents a row with column names as keys
     * @throws SQLException if a database access error occurs
     */
     List<Map<String, Object>> getTableOrViewData(Connection connection, String schemaName, String name) throws SQLException;

    /**
     * Fetches table or view data from the database and applies obfuscation rules to the specified columns.
     *
     * @param connection      The database connection object.
     * @param name            The name of the table or view.
     * @param schemaName The name of the schema where the table is located.
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of maps representing the rows of the table or view with the specified obfuscation rules applied.
     * @throws SQLException if a database access error occurs
     */
     List<Map<String, Object>> getDataWithMask(Connection connection, String schemaName, String name, Map<String, ObfuscationRule> obfuscationRules) throws SQLException;

    /**
     * This function retrieves data from a table or view in the database. It supports both pagination and selection of specific columns.
     *
     * @param connection The database connection object.
     * @param schemaName The schema name of the table or view.
     * @param tableName The name of the table or view.
     * @param columnList A list of columns to select. If null or empty, all columns will be selected.
     * @param pageOffset The offset for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @param pageSize The size of a page for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @return A Map object containing the retrieved data in 'results' and the total pages of data in 'totalPages'.
     * @throws SQLException If a database access error occurs.
     */
    Map<String, Object> getDataWithPage(Connection connection, String schemaName, String tableName,
                                        List<String> columnList, int pageOffset, int pageSize) throws SQLException;

    /**
     * This function retrieves data from a table or view in the database. It supports both pagination and selection of specific columns.
     *
     * @param connection The database connection object.
     * @param schemaName The schema name of the table or view.
     * @param tableName The name of the table or view.
     * @param columnList A list of columns to select. If null or empty, all columns will be selected.
     * @param pageOffset The offset for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @param pageSize The size of a page for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A Map object containing the retrieved data in 'results' and the total pages of data in 'totalPages'.
     * @throws SQLException If a database access error occurs.
     */
    Map<String, Object> getDataWithPage(Connection connection, String schemaName, String tableName,
                                               List<String> columnList, int pageOffset, int pageSize,
                                               Map<String, ObfuscationRule> obfuscationRules) throws SQLException;

    /**
     * Executes the specified database function with the given parameters.
     *
     * @param connection   The database connection (java.sql.Connection) to execute the function on
     * @param schemaName The name of the schema where the table is located.
     * @param functionName The name of the function to execute
     * @param params       The list of parameters to pass to the function
     * @return A List of Maps containing the function result, where each Map represents a row with column names as keys
     * @throws SQLException if a database access error occurs
     */
     List<Map<String, Object>> executeFunction(Connection connection, String schemaName, String functionName, Object... params) throws SQLException;

    /**
     * Scans a database table or view for sensitive data based on a list of regular expressions.
     *
     * @param connection The SQL connection to the database.
     * @param schemaName The name of the schema where the table is located.
     * @param tableName  The name of the table or view to scan.
     * @param regexList  A list of regular expressions used for matching sensitive data.
     * @return A list of {@link SensitiveColumn} instances containing matched sensitive data.
     *         Only the columns that actually matched sensitive data will be returned.
     *         For each SensitiveColumn, up to 5 matched data will be stored in the matchData attribute.
     * @throws SQLException if a database access error occurs
     */
    List<SensitiveColumn> scanTableData(Connection connection, String schemaName, String tableName, List<String> regexList) throws SQLException;
}
