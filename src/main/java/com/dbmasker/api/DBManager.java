package com.dbmasker.api;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.data.TableIndex;
import com.dbmasker.data.TableMetaData;
import com.dbmasker.database.Database;
import com.dbmasker.database.DatabaseFactory;
import com.dbmasker.utils.ErrorMessages;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * DBManager is the main class for the DBMasker's metadata and data API.
 * It provides methods to scan database tables and views for sensitive data and to apply obfuscation rules to the data.
 */
public class DBManager {

    /**
     * Default constructor for DBManager class.
     */
    private DBManager() {
        throw new UnsupportedOperationException();
    }

    /**
     * Tests the connection to a database.
     *
     * @param driver   The fully-qualified name of the JDBC driver class.
     * @param url      The JDBC URL for the database.
     * @param username The username for the database connection.
     * @param password The password for the database connection.
     * @return A boolean value indicating whether the connection was successful (true) or not (false).
     * @throws SQLException if the connection cannot be established due to SQL-related issues.
     * @throws ClassNotFoundException if the JDBC driver class is not found.
     */
    public static boolean testConnection(String driver, String url, String username, String password)
            throws SQLException, ClassNotFoundException {
        boolean connectionSuccess = false;
        try (Connection connection = createConnection(driver, url, username, password)) {
            // Attempt to establish a connection to the database
            connectionSuccess = true;
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException(ErrorMessages.JDBC_DRIVER_NOT_FOUND_ERROR + e.getMessage());
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.CONNECTION_ESTABLISHMENT_FAILURE_ERROR + e.getMessage());
        }
        return connectionSuccess;
    }

    /**
     * Creates a database connection using the provided parameters.
     *
     * @param driver   The fully-qualified name of the JDBC driver class.
     * @param url      The JDBC URL for the database.
     * @param username The username for the database connection.
     * @param password The password for the database connection.
     * @return A Connection object if the connection is established successfully; otherwise, returns null.
     * @throws SQLException if the connection cannot be established due to SQL-related issues.
     * @throws ClassNotFoundException if the JDBC driver class is not found.
     */
    public static Connection createConnection(String driver, String url, String username, String password)
            throws SQLException, ClassNotFoundException {
        Connection connection = null;
        try {
            // Load the specified JDBC driver
            Class.forName(driver);

            // Attempt to establish a connection to the database
            connection = DriverManager.getConnection(url, username, password);

            // If connection is successful, return true
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException(ErrorMessages.JDBC_DRIVER_NOT_FOUND_ERROR + e.getMessage());
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.CONNECTION_ESTABLISHMENT_FAILURE_ERROR + e.getMessage());
        }
        return connection;
    }

    /**
     * Closes a database connection.
     *
     * @param connection The Connection object to be closed.
     * @return A boolean value representing whether the connection was closed successfully.
     * @throws SQLException if the connection cannot be closed due to SQL-related issues.
     */
    public static boolean closeConnection(Connection connection) throws SQLException {
        if (connection != null) {
            try {
                connection.close();
                return true;
            } catch (SQLException e) {
                throw new SQLException(ErrorMessages.FAILED_TO_CLOSE_CONNECTION_ERROR + e.getMessage());
            }
        }
        return false;
    }

    /**
     * Retrieves the schema names for the specified database type using the provided connection.
     *
     * @param connection A java.sql.Connection object representing the connection to the database.
     * @param dbType     A string representing the database type (e.g., "sqlite", "mysql").
     * @return A list of schema names as strings.
     * @throws SQLException if a database access error occurs
     */
    public static List<String> getSchema(Connection connection, String dbType) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getSchemas(connection);
    }

    /**
     * Retrieves the names of all tables within the specified schema of a database.
     *
     * @param connection the java.sql.Connection object for the database
     * @param dbType     the type of the database (e.g., "sqlite", "mysql")
     * @param schemaName the name of the schema for which to retrieve table names
     * @return a list of table names within the specified schema
     * @throws SQLException if a database access error occurs
     */
    public static List<String> getTables(Connection connection, String dbType, String schemaName) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getTables(connection, schemaName);
    }

    /**
     * Retrieves the names of all views within the specified schema of a database.
     *
     * @param connection the java.sql.Connection object for the database
     * @param dbType     the type of the database (e.g., "sqlite", "mysql")
     * @param schemaName the name of the schema for which to retrieve view names
     * @return a list of view names within the specified schema
     * @throws SQLException if a database access error occurs
     */
    public static List<String> getViews(Connection connection, String dbType, String schemaName) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getViews(connection, schemaName);
    }

    /**
     * Retrieves a list of functions in the specified schema.
     *
     * @param connection A valid database connection.
     * @param dbType The database type, such as "sqlite", "mysql", etc.
     * @param schemaName The name of the specified schema.
     * @return Returns a list of DatabaseFunction objects, containing all the functions in the given schema.
     * @throws SQLException if a database access error occurs
     */
    public static List<DatabaseFunction> getFuncs(Connection connection, String dbType, String schemaName) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getFuncs(connection, schemaName);
    }

    /**
     * Retrieves metadata for tables in a given schema.
     *
     * @param connection A valid database connection.
     * @param dbType The database type, such as "sqlite", "mysql", etc.
     * @param schemaName The name of the schema for which metadata is to be retrieved.
     * @return A list of TableMetaData objects containing metadata for the tables in the given schema.
     * @throws SQLException if a database access error occurs
     */
    public static List<TableMetaData> getMetaData(Connection connection, String dbType, String schemaName) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getMetaData(connection, schemaName);
    }

    /**
     * Retrieves table attributes for a given table in the specified schema.
     *
     * @param connection  A valid database connection.
     * @param dbType      The database type, such as "sqlite", "mysql", etc.
     * @param schemaName  The schema name where the table is located.
     * @param table       The table for which attributes are to be retrieved.
     * @return A list of TableAttribute objects containing the attributes of the given table.
     * @throws SQLException if a database access error occurs
     */
    public static List<TableAttribute> getTableAttribute(Connection connection, String dbType, String schemaName,
                                                         String table) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (table == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getTableAttribute(connection, schemaName, table);
    }

    /**
     * Retrieves the primary keys of a specified table within the given schema.
     *
     * @param connection A valid database connection.
     * @param dbType the type of the database, such as "sqlite", "mysql", etc.
     * @param schemaName The specified schema name.
     * @param table The specified table name.
     * @return Returns a list of primary key column names for the specified table.
     * @throws SQLException if a database access error occurs
     */
    public static List<String> getPrimaryKeys(Connection connection, String dbType,
                                                         String schemaName, String table) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (table == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getPrimaryKeys(connection, schemaName, table);
    }


    /**
     * Retrieves the unique keys of the specified database table.
     * @param connection a valid database connection.
     * @param dbType the type of the database, such as "sqlite", "mysql", etc.
     * @param schemaName the specified schema name.
     * @param table the specified table name.
     * @return a Map where the key is the name of the unique key and the value is a Set of the column names included in the unique key.
     * @throws SQLException if a database access error occurs
     */
    public static Map<String, Set<String>> getUniqueKeys(Connection connection, String dbType,
                                                         String schemaName, String table) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (table == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getUniqueKeys(connection, schemaName, table);
    }

    /**
     * Retrieves a list of indices for the specified table in the given schema.
     * @param connection A valid database connection.
     * @param dbType The type of the database, such as "sqlite", "mysql", etc.
     * @param schemaName The name of the schema where the table is located.
     * @param table The name of the table for which to retrieve the indices.
     * @return A list of {@link TableIndex} objects, each containing information about an index in the specified table.
     * @throws SQLException if a database access error occurs
     */
    public static List<TableIndex> getIndex(Connection connection, String dbType, String schemaName, String table) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (table == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getIndex(connection, schemaName, table);
    }

    /**
     * Executes the given update SQL query using the provided database connection.
     *
     * @param connection The database connection to use for executing the query.
     * @param dbType     The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param sql        The SQL query string to execute.
     * @return rowCount  The number of rows affected by the query.
     * @throws SQLException if a database access error occurs
     */
    public static int executeUpdateSQL(Connection connection, String dbType, String sql) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (sql == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.executeUpdateSQL(connection, sql);
    }

    /**
     * Executes a batch of update SQL statements and returns the number of successfully updated records.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the batch update
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param sqlList The list of update SQL statements to be executed
     * @return rowCount The number of records successfully updated (int)
     * @throws SQLException if a database access error occurs
     */
    public static int executeUpdateSQLBatch(Connection connection, String dbType, List<String> sqlList) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (sqlList == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_LIST_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.executeUpdateSQLBatch(connection, sqlList);
    }

    /**
     * Executes a SQL query and returns the results as a list of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param sql The SQL query string (String) to be executed
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data
     * @throws SQLException if a database access error occurs
     */
    public static List<Map<String, Object>> executeQuerySQL(Connection connection, String dbType, String sql) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (sql == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.executeQuerySQL(connection, sql);
    }

    /**
     * Executes a batch of SQL queries and returns the results as a list of lists of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param sqlList The list of SQL query strings to be executed
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data
     * @throws SQLException if a database access error occurs
     */
    public static List<List<Map<String, Object>>> executeQuerySQLBatch(Connection connection, String dbType, List<String> sqlList) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (sqlList == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_LIST_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.executeQuerySQLBatch(connection, sqlList);
    }

    /**
     * Executes a SQL query or update statement, returns the results as a list of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param sql The SQL query or update statement to be executed
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    public static List<Map<String, Object>> executeSQL(Connection connection, String dbType, String sql) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (sql == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.executeSQL(connection, sql);
    }

    /**
     * Executes a batch of SQL query or update statement, and returns the results as a list of lists of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param sqlList The list of SQL query or update statements to be executed
     * @return A list of lists of maps where each inner list represents a result. For a query statement, each map
     *         representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    public static List<List<Map<String, Object>>> executeSQLBatch(Connection connection, String dbType, List<String> sqlList) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (sqlList == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_LIST_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.executeSQLBatch(connection, sqlList);
    }

    /**
     * Executes a script of SQL query or update statement, and returns the results as a list of lists of maps.
     * The script is split into individual statements using the semicolon as a delimiter.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param sqlScript The script of SQL query or update statements to be executed, separated by semicolons
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    public static List<List<Map<String, Object>>> executeSQLScript(Connection connection, String dbType, String sqlScript) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (sqlScript == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_SCRIPT_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.executeSQLScript(connection, sqlScript);
    }

    /**
     * Commits a transaction for the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) on which the transaction is being executed
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.)
     * @return true if the transaction was successfully committed, false otherwise
     * @throws SQLException if a database access error occurs
     */
    public static boolean commit(Connection connection, String dbType) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.commit(connection);
    }

    /**
     * Sets the auto-commit mode for the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) for which the auto-commit mode should be set
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.)
     * @param autoCommit The auto-commit mode (boolean) to set for the connection
     * @return true if the auto-commit mode was successfully set, false otherwise
     * @throws SQLException if a database access error occurs
     */
    public static boolean setAutoCommit(Connection connection, String dbType, boolean autoCommit) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.setAutoCommit(connection, autoCommit);
    }

    /**
     * Rolls back a transaction for the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) on which the transaction is being executed
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.)
     * @return true if the transaction was successfully rolled back, false otherwise
     * @throws SQLException if a database access error occurs
     */
    public static boolean rollback(Connection connection, String dbType) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.rollback(connection);
    }

    /**
     * Fetches data from the specified table or view in the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) to fetch the data from
     * @param dbType     The type of the database (e.g., "sqlite", "mysql", etc.)
     * @param schemaName The name of the schema where the table is located.
     * @param name  The name of the table/view to fetch the data from
     * @return A List of Maps containing the table/view data, where each Map represents a row with column names as keys
     * @throws SQLException if a database access error occurs
     */
    public static List<Map<String, Object>> getTableOrViewData(Connection connection, String dbType, String schemaName, String name) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (name == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getTableOrViewData(connection, schemaName, name);
    }

    /**
     * This function retrieves data from a table or view in the database. It supports both pagination and selection of specific columns.
     *
     * @param connection The database connection object.
     * @param dbType     The type of the database (e.g., "sqlite", "mysql", etc.)
     * @param schemaName The schema name of the table or view.
     * @param tableName The name of the table or view.
     * @param columnList A list of columns to select. If null or empty, all columns will be selected.
     * @param pageOffset The offset for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @param pageSize The size of a page for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @return A Map object containing the retrieved data in 'results' and the total pages of data in 'totalPages'.
     * @throws SQLException If a database access error occurs.
     */
    public static Map<String, Object> getDataWithPage(Connection connection, String dbType, String schemaName, String tableName,
                                        List<String> columnList, int pageOffset, int pageSize) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (tableName == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getDataWithPage(connection, schemaName, tableName, columnList, pageOffset, pageSize);
    }

    /**
     * Executes the specified database function with the given parameters.
     *
     * @param connection   The database connection (java.sql.Connection) to execute the function on
     * @param dbType       The type of the database (e.g., "sqlite", "mysql", etc.)
     * @param schemaName The name of the schema where the table is located.
     * @param functionName The name of the function to execute
     * @param params       The list of parameters to pass to the function
     * @return A List of Maps containing the function result, where each Map represents a row with column names as keys
     * @throws SQLException if a database access error occurs
     */
    public static List<Map<String, Object>> executeFunction(Connection connection, String dbType, String schemaName,
                                                            String functionName, Object... params) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.executeFunction(connection, schemaName, functionName, params);
    }

}