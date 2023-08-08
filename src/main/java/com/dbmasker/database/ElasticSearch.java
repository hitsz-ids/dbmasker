package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ElasticSearch Database class implements the Database interface for ElasticSearch databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a ElasticSearch database.
 */
public class ElasticSearch extends BaseDatabase {

     /**
     * Default constructor for ElasticSearch class.
     */
    public ElasticSearch() {
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
        throw new SQLFeatureNotSupportedException("ElasticSearch does not support schemas");
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
        List<String> tables = new ArrayList<>();
        ResultSet resultSet = null;

        try (Statement statement = connection.createStatement();) {
            String sql = "SHOW TABLES";
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                String tableName = resultSet.getString("name");
                tables.add(tableName);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.TABLE_NAMES_RETRIEVAL_ERROR, e);
        } finally {
            DbUtils.closeResultSet(resultSet);
        }

        return tables;
    }

    /**
     * Retrieves a list of functions in the specified schema.
     *
     * @param connection A valid database connection.
     * @param schemaName The name of the specified schema.
     * @return Returns a list of DatabaseFunction objects, containing all the functions in the given schema.
     * @throws SQLFeatureNotSupportedException if the database does not support this method
     */
    @Override
    public List<DatabaseFunction> getFuncs(Connection connection, String schemaName) throws SQLException {
        throw new SQLFeatureNotSupportedException("ElasticSearch does not support functions");
    }

    /**
     * Executes a SQL query or update statement, returns the results as a list of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param sql The SQL query or update statement to be executed
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<Map<String, Object>> executeSQL(Connection connection, String sql) throws SQLException {
        return super.executeQuerySQL(connection, sql);
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
}
