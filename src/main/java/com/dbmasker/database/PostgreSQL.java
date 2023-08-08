package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL Database class implements the Database interface for PostgreSQL databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a PostgreSQL database.
 */
public class PostgreSQL extends BaseDatabase {

    /**
     * Default constructor for PostgreSQL class.
     */
    public PostgreSQL() {
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
        String query = "SELECT schema_name FROM information_schema.schemata";
       return DbUtils.getSchemasFromSQL(connection, query);
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
            sql = "SELECT table_name FROM information_schema.tables";
        } else {
            sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ?";
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                preparedStatement.setString(1, schemaName);
            }
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                tableList.add(resultSet.getString("table_name"));
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
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<DatabaseFunction> getFuncs(Connection connection, String schemaName) throws SQLException {
        List<DatabaseFunction> functionList = new ArrayList<>();
        ResultSet resultSet = null;
        String sql;
        if (schemaName == null) {
            sql = "SELECT n.nspname as schema_name, p.proname as function_name " +
                    "FROM pg_proc p " +
                    "JOIN pg_namespace n ON p.pronamespace = n.oid";
        } else {
            sql = "SELECT n.nspname as schema_name, p.proname as function_name " +
                    "FROM pg_proc p " +
                    "JOIN pg_namespace n ON p.pronamespace = n.oid " +
                    "WHERE n.nspname = ?";
        }
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                preparedStatement.setString(1, schemaName);
            }
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String functionName = resultSet.getString("function_name");
                functionList.add(new DatabaseFunction(schemaName, functionName));
            }

        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.FUNCTION_NAMES_RETRIEVAL_ERROR, e);
        } finally {
            DbUtils.closeResultSet(resultSet);
        }

        return functionList;
    }
}
