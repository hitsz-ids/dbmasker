package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * MsSQL Database class implements the Database interface for Microsoft SQL Server databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a MsSQL database.
 */
public class MsSQL extends BaseDatabase {

    /**
     * Default constructor for MsSQL class.
     */
    public MsSQL() {
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

        String query = "SELECT name FROM sys.schemas;";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {
                schemaList.add(resultSet.getString("name"));
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
            sql = "SELECT table_name FROM information_schema.tables;";
        } else {
            sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ?;";
        }
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                pstmt.setString(1, schemaName);
            }
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    tableList.add(resultSet.getString("table_name"));
                }
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.TABLE_NAMES_RETRIEVAL_ERROR, e);
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
        String sql;
        if (schemaName == null) {
            sql = "SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME FROM information_schema.routines " +
                    "WHERE ROUTINE_TYPE = 'FUNCTION'";
        } else {
            sql = "SELECT SPECIFIC_SCHEMA, SPECIFIC_NAME FROM information_schema.routines " +
                    "WHERE ROUTINE_TYPE = 'FUNCTION' AND SPECIFIC_SCHEMA = ?";
        }
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                pstmt.setString(1, schemaName);
            }

            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    String specificSchema = resultSet.getString("SPECIFIC_SCHEMA");
                    String functionName = resultSet.getString("SPECIFIC_NAME");
                    functionList.add(new DatabaseFunction(specificSchema, functionName));
                }
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.FUNCTION_NAMES_RETRIEVAL_ERROR, e);
        }

        return functionList;
    }

    /**
     * Executes the specified SQL query and applies obfuscation rules to the specified columns.
     * @param columns The columns to fetch from the table/view.
     * @param tableName The name of the table/view to fetch the data from.
     * @param pageSize The number of records to fetch.
     * @param pageOffset The offset of the first record to fetch.
     * @return The SQL query to execute.
     */
    @Override
    protected String getQueryWithPage(String columns, String tableName, int pageSize, int pageOffset) {
        int offset = (pageOffset - 1) * pageSize;
        return String.format("SELECT %s FROM %s ORDER BY (SELECT NULL) OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", columns, tableName, offset, pageSize);
    }
}
