package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Hive Database class implements the Database interface for Hive databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a Hive database.
 */
public class Hive extends BaseDatabase {

    /**
     * Default constructor for Hive class.
     */
    public Hive() {
        super();
    }

    /**
     * Retrieves a list of schema names from the given database connection.
     *
     * @param connection A java.sql.Connection object representing the database connection.
     * @return A list of schema names as strings.
     * @throws SQLFeatureNotSupportedException if the database does not support this method
     */
    @Override
    public List<String> getSchemas(Connection connection) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Hive does not support schemas");
    }

    /**
     * Retrieves the names of all tables within the specified schema of a database.
     *
     * @param connection the java.sql.Connection object for the database
     * @param schemaName the name of the schema for which to retrieve table names
     * @return a list of table names within the specified schema
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<String> getTables(Connection connection, String schemaName) throws SQLException {
        List<String> tableList = new ArrayList<>();

        String query = "SHOW TABLES";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                tableList.add(tableName);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.TABLE_NAMES_RETRIEVAL_ERROR, e);
        }

        return tableList;
    }

    /**
     * Executes a batch of update SQL statements and returns the number of successfully updated records.
     * No transaction support in Gbase-8s
     *
     * @param connection The database connection (java.sql.Connection) used to execute the batch update
     * @param sqlList The list of update SQL statements to be executed
     * @return rowCount The number of records successfully updated (int)
     * @throws SQLException if a database access error occurs
     */
    @Override
    public int executeUpdateSQLBatch(Connection connection, List<String> sqlList) throws SQLException {
        int rowCount = 0;
        try (Statement stmt = connection.createStatement()) {
            for (String sql : sqlList) {
                rowCount += stmt.executeUpdate(sql);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.BATCH_UPDATE_SQL_EXECUTION_ERROR, e);
        }
        return rowCount;
    }

    /**
     * Executes a batch of SQL query or update statement, and returns the results as a list of lists of maps.
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
    @Override
    public List<List<Map<String, Object>>> executeSQLBatch(Connection connection, List<String> sqlList, Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        return executeSQLBatchNoTransaction(connection, sqlList, obfuscationRules);
    }

    /**
     * Retrieves a list of functions in the specified schema.
     *
     * @param connection A valid database connection.
     * @param schemaName The name of the specified schema.
     * @return Returns a list of DatabaseFunction objects, containing all the functions in the given schema.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<DatabaseFunction> getFuncs(Connection connection, String schemaName) throws SQLException {
        List<DatabaseFunction> functions = new ArrayList<>();
        String query = "SHOW FUNCTIONS";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                DatabaseFunction function = new DatabaseFunction(schemaName, rs.getString(1));
                functions.add(function);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.FUNCTION_NAMES_RETRIEVAL_ERROR, e);
        }

        return functions;
    }
}
