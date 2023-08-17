package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Phoenix Database class implements the Database interface for Phoenix databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a Phoenix database.
 */
public class Phoenix extends BaseDatabase {

    /**
     * Default constructor for Phoenix class.
     */
    public Phoenix() {
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
        String sql = "SELECT DISTINCT TABLE_SCHEM FROM SYSTEM.CATALOG";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
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
        ResultSet resultSet = null;
        String sql;
        if (schemaName == null) {
            sql = "SELECT DISTINCT TABLE_NAME FROM SYSTEM.CATALOG";
        } else {
            sql = "SELECT DISTINCT TABLE_NAME FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = ?";
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                preparedStatement.setString(1, schemaName);
            }

            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                tableList.add(tableName);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.TABLE_NAMES_RETRIEVAL_ERROR, e);
        } finally {
            DbUtils.closeResultSet(resultSet);
        }

        return tableList;
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
    public List<DatabaseFunction> getFuncs(Connection connection, String schemaName) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Phoenix does not support functions");
    }


    /**
     * Retrieves the primary keys of a specified table within the given schema.
     *
     * @param connection A valid database connection.
     * @param schemaName The specified schema name.
     * @param table The specified table name.
     * @return Returns a Set of primary key column names for the specified table.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Set<String> getPrimaryKeys(Connection connection, String schemaName, String table) throws SQLException {
        Set<String> primaryKeys = new HashSet<>();

        // Use a query to get the primary key column from the SYSTEM.CATALOG table for the given table and schema
        String sql = "SELECT COLUMN_NAME " +
                "FROM SYSTEM.CATALOG " +
                "WHERE TABLE_SCHEM = ? " +
                "AND TABLE_NAME = ? " +
                "AND COLUMN_FAMILY IS NULL " +  // Primary key columns have NULL COLUMN_FAMILY
                "AND KEY_SEQ IS NOT NULL " +    // Indicates the column is part of the primary key
                "ORDER BY KEY_SEQ";             // Order by the key sequence to get the first part of compound primary key if any

        // try-with-resources block ensures that both the PreparedStatement and ResultSet are closed automatically
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            // Set parameters for the PreparedStatement to prevent SQL injection
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, table);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {

                // Loop over the resultSet to get all primary key columns
                while (resultSet.next()) {
                    primaryKeys.add(resultSet.getString("COLUMN_NAME"));
                }
            }
        }
        return primaryKeys;
    }
}
