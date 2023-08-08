package com.dbmasker.api;

import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.data.SensitiveColumn;
import com.dbmasker.database.Database;
import com.dbmasker.database.DatabaseFactory;
import com.dbmasker.utils.ErrorMessages;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * DBSecManager is the main class for the DBMasker's security API.
 */
public class DBSecManager {

    /**
     * Default constructor for DBSecManager class.
     */
    private DBSecManager() {
        throw new UnsupportedOperationException();
    }

    /**
     * The default number of columns to match when scanning for sensitive data.
     */
    public static final int MATCH_DATA_SIZE = 5;

    /**
     * Fetches table or view data from the database and applies obfuscation rules to the specified columns.
     *
     * @param connection The database connection object.
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param schemaName The name of the schema where the table is located.
     * @param name The name of the table or view.
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of maps representing the rows of the table or view with the specified obfuscation rules applied.
     * @throws SQLException if a database access error occurs
     */
    public static List<Map<String, Object>> getDataWithMask(Connection connection, String dbType, String schemaName, String name,
                                                            Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (name == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        if (obfuscationRules == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getDataWithMask(connection, schemaName, name, obfuscationRules);
    }

    /**
     * This function retrieves data from a table or view in the database. It supports both pagination and selection of specific columns.
     *
     * @param connection The database connection object.
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param schemaName The schema name of the table or view.
     * @param tableName The name of the table or view.
     * @param columnList A list of columns to select. If null or empty, all columns will be selected.
     * @param pageOffset The offset for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @param pageSize The size of a page for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A Map object containing the retrieved data in 'results' and the total pages of data in 'totalPages'.
     * @throws SQLException If a database access error occurs.
     */
    public static Map<String, Object> getDataWithPageAndMask(Connection connection, String dbType, String schemaName, String tableName,
                                        List<String> columnList, int pageOffset, int pageSize,
                                        Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (obfuscationRules == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        if (tableName == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.getDataWithPage(connection, schemaName, tableName, columnList, pageOffset, pageSize, obfuscationRules);
    }

    /**
     * Executes a SQL query and returns the results as a list of maps, applies obfuscation rules to the specified columns.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param sql The SQL query string (String) to be executed
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data
     * @throws SQLException if a database access error occurs
     */
    public static List<Map<String, Object>> execQuerySQLWithMask(Connection connection, String dbType, String sql,
                                                                 Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (sql == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_ERROR);
        }

        if (obfuscationRules == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.execQuerySQLWithMask(connection, sql, obfuscationRules);
    }


    /**
     * Executes a script of SQL query or update statement, and returns the results as a list of lists of maps.
     * The script is split into individual statements using the semicolon as a delimiter.
     * Applies obfuscation rules to the specified columns.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param dbType The type of the database (e.g., "sqlite", "mysql", etc.).
     * @param sqlScript The script of SQL query or update statements to be executed, separated by semicolons
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    public static List<List<Map<String, Object>>> execSQLScriptWithMask(Connection connection, String dbType, String sqlScript,
                                                                 Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (sqlScript == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_SCRIPT_ERROR);
        }

        if (obfuscationRules == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.executeSQLScript(connection, sqlScript, obfuscationRules);
    }

    /**
     * Scans a database table or view for sensitive data based on a list of regular expressions.
     *
     * @param connection The SQL connection to the database.
     * @param dbType     The type of the database (e.g., SQLite, MySQL, PostgreSQL).
     * @param schemaName The name of the schema where the table is located.
     * @param tableName  The name of the table or view to scan.
     * @param regexList  A list of regular expressions used for matching sensitive data.
     * @return A list of {@link SensitiveColumn} instances containing matched sensitive data.
     *         Only the columns that actually matched sensitive data will be returned.
     *         For each SensitiveColumn, up to 5 matched data will be stored in the matchData attribute.
     * @throws SQLException if a database access error occurs
     */
    public static List<SensitiveColumn> scanTableData(Connection connection, String dbType, String schemaName,
                                                      String tableName, List<String> regexList) throws SQLException {
        if (connection == null || dbType == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR);
        }

        if (tableName == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        if (regexList == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_REGEX_LIST_ERROR);
        }

        Database database = new DatabaseFactory().getDatabase(dbType);
        return database.scanTableData(connection, schemaName, tableName, regexList);
    }

}
