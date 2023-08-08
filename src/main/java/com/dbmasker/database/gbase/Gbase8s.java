package com.dbmasker.database.gbase;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.database.BaseDatabase;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * GBase8s Database class implements the Database interface for GBase8s databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a GBase8s database.
 */
public class Gbase8s extends BaseDatabase {

    /**
     * Default constructor for GBase8s class.
     */
    public Gbase8s() {
        super();
    }

    /**
     * Retrieves a list of schema names from the given database connection.
     * the schema of Gbase-8s equals to the database.
     *
     * @param connection A java.sql.Connection object representing the database connection.
     * @return A list of schema names as strings.
     * @throws SQLFeatureNotSupportedException if the database does not support this method
     */
    @Override
    public List<String> getSchemas(Connection connection) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Gbase-8s does not support schema");
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
        String query = "SELECT tabname FROM systables WHERE tabid>99";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("tabname");
                    tables.add(tableName);
                }
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.TABLE_NAMES_RETRIEVAL_ERROR, e);
        }

        return tables;
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
        try (Statement stmt = connection.createStatement()){
            // Add each SQL statement to the batch
            for (String sql : sqlList) {
                stmt.addBatch(sql);
            }

            // Execute the batch of SQL statements
            int[] updateCounts = stmt.executeBatch();

            // Calculate the number of records successfully updated
            for (int count : updateCounts) {
                if (count != Statement.EXECUTE_FAILED) {
                    rowCount += count;
                }
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

        String query = "SELECT procname, mode FROM sysprocedures WHERE isproc = 'f' AND mode='O'";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    DatabaseFunction function = new DatabaseFunction(schemaName,  resultSet.getString("procname"));
                    functions.add(function);
                }
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.FUNCTION_NAMES_RETRIEVAL_ERROR, e);
        }
        return functions;
    }

    /**
     * Executes the specified database function with the given parameters.
     *
     * @param connection   The database connection (java.sql.Connection) to execute the function on
     * @param schemaName The name of the schema where the table is located.
     * @param functionName The name of the function to execute
     * @param params       The list of parameters to pass to the function
     * @return A List of Maps containing the function result, where each Map represents a row with column names as keys
     * @throws SQLException If an error occurs while executing the function
     */
    @Override
    public List<Map<String, Object>> executeFunction(Connection connection, String schemaName, String functionName,
                                                     Object... params) throws SQLException {
        return super.executeFunction(connection, "EXECUTE FUNCTION ", schemaName, functionName, params);
    }
}
