package com.dbmasker.database.gbase;

import com.dbmasker.data.*;
import com.dbmasker.database.BaseDatabase;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * GBase8a Database class implements the Database interface for GBase8a databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a GBase8a database.
 */
public class GBase8a extends BaseDatabase {

    /**
     * Default constructor for GBase8a class.
     */
    public GBase8a() {
        super();
    }

    /**
     * Retrieves a list of schema names from the given database connection.
     *
     * @param connection A java.sql.Connection object representing the database connection.
     * @return A list of schema names as strings.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<String> getSchemas(Connection connection) throws SQLException {
        List<String> schemaList = new ArrayList<>();
        String query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String schemaName = resultSet.getString("SCHEMA_NAME");
                schemaList.add(schemaName);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.SCHEMA_NAMES_RETRIEVAL_ERROR, e);
        }

        return schemaList;
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
        String sql;
        if (schemaName == null) {
            sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES";
        } else {
            sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?";
        }

        getResultList(connection, schemaName, tableList, sql);

        return tableList;
    }

    /**
     * Retrieves the names of all views within the specified schema.
     *
     * @param connection the java.sql.Connection object for the database
     * @param schemaName the name of the schema for which to retrieve view names
     * @return a list of view names within the specified schema
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<String> getViews(Connection connection, String schemaName) throws SQLException {
        List<String> viewList = new ArrayList<>();
        String sql;
        if (schemaName == null) {
            sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS";
        } else {
            sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = ?";
        }

        getResultList(connection, schemaName, viewList, sql);
        return viewList;
    }

    /**
     * Retrieves metadata for tables in a given schema.
     * The usage of catalog and schemaName is reversed compared to other databases.
     *
     * @param connection A valid database connection.
     * @param schemaName The name of the schema for which metadata is to be retrieved.
     * @return A list of TableMetaData objects containing metadata for the tables in the given schema.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<TableMetaData> getMetaData(Connection connection, String schemaName) throws SQLException {
        return BaseDatabase.getMetaData(connection, schemaName, "");
    }

    /**
     * Retrieves table attributes for a given table in the specified schema.
     * The usage of catalog and schemaName is reversed compared to other databases.
     *
     * @param connection  A valid database connection.
     * @param schemaName  The schema name where the table is located.
     * @param table       The table for which attributes are to be retrieved.
     * @return A list of TableAttribute objects containing the attributes of the given table.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<TableAttribute> getTableAttribute(Connection connection, String schemaName, String table) throws SQLException {
        return BaseDatabase.getTableAttribute(connection, schemaName, "", table);
    }

    /**
     * Retrieves the primary keys of a specified table within the given schema.
     *
     * @param connection A valid database connection.
     * @param schemaName The specified schema name.
     * @param table The specified table name.
     * @return Returns a list of primary key column names for the specified table.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<String> getPrimaryKeys(Connection connection, String schemaName, String table) throws SQLException {
        return getPrimaryKeys(connection, schemaName, "", table);
    }

    /**
     * Retrieves the unique keys of a specified database table.
     *
     * @param connection A valid database connection.
     * @param schemaName The specified schema name.
     * @param table The specified table name.
     * @return Returns a Map, where the key is the unique key name, and the value is a collection of unique key column names.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Map<String, Set<String>> getUniqueKeys(Connection connection, String schemaName, String table) throws SQLException {
        return getUniqueKeys(connection, schemaName, "", table);
    }

    /**
     * Retrieves a list of indices for the specified table in the given schema.
     * @param connection A valid database connection.
     * @param schemaName The name of the schema where the table is located.
     * @param table The name of the table for which to retrieve the indices.
     * @return A list of {@link TableIndex} objects, each containing information about an index in the specified table.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<TableIndex> getIndex(Connection connection, String schemaName, String table) throws SQLException {
        return getIndex(connection, schemaName, "", table);
    }

    /**
     * Executes a batch of SQL query or update statement, and returns the results as a list of lists of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlList The list of SQL query or update statements to be executed
     * @param obfuscationRules A map of obfuscation rules, where the key is the table name and the value is the corresponding obfuscation rule.
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
        List<DatabaseFunction> funcs = new ArrayList<>();
        String sql;
        if (schemaName == null) {
            sql = "SELECT ROUTINE_SCHEMA, ROUTINE_NAME " +
                  "FROM INFORMATION_SCHEMA.ROUTINES " +
                  "WHERE ROUTINE_TYPE = 'FUNCTION'";
        } else {
            sql = "SELECT ROUTINE_SCHEMA, ROUTINE_NAME " +
                  "FROM INFORMATION_SCHEMA.ROUTINES " +
                  "WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_SCHEMA = ?";
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                preparedStatement.setString(1, schemaName);
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    DatabaseFunction function = new DatabaseFunction(resultSet.getString("ROUTINE_SCHEMA").trim(),
                                                                     resultSet.getString("ROUTINE_NAME").trim());
                    funcs.add(function);
                }
            }
        } catch (SQLException e) {
            throw new SQLException("Unable to retrieve functions", e);
        }

        return funcs;
    }

    /**
     * This method retrieves a list of results (e.g. view or table names) from the database based on a given query.
     *
     * @param connection The Connection object used to connect to the database.
     * @param schemaName The name of the schema in the database.
     * @param tableList   The list of results (e.g. view or table names) to be populated.
     * @param sql      The SQL query to be executed to retrieve the results.
     * @throws SQLException Thrown if there is an error executing the query.
     */
    private void getResultList(Connection connection, String schemaName, List<String> tableList, String sql) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                preparedStatement.setString(1, schemaName);
            }

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    tableList.add(tableName);
                }
            }
        } catch (SQLException e) {
            throw new SQLException(e);
        }
    }

}
