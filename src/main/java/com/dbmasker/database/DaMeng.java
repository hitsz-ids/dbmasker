package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * DaMeng Database class implements the Database interface for DaMeng databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a DaMeng database.
 */
public class DaMeng extends BaseDatabase {

    /**
     * Default constructor for DaMeng class.
     */
    public DaMeng() {
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
        List<String> schemas = new ArrayList<>();

        String sql = "SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OBJECT_TYPE='SCH';";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String schemaName = rs.getString("OBJECT_NAME");
                schemas.add(schemaName);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.SCHEMA_NAMES_RETRIEVAL_ERROR, e);
        }

        return schemas;
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
            sql = "SELECT TABLE_NAME FROM DBA_TABLES";
        } else {
            sql = "SELECT TABLE_NAME FROM DBA_TABLES WHERE OWNER = ?";
        }
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                pstmt.setString(1, schemaName.toUpperCase());
            }
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    tableList.add(rs.getString("TABLE_NAME"));
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
        List<DatabaseFunction> functions = new ArrayList<>();
        String sql;
        if (schemaName == null) {
            sql = "SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OBJECT_TYPE='FUNCTION'";
        } else {
            sql = "SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OBJECT_TYPE='FUNCTION' AND OWNER=?";
        }
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                pstmt.setString(1, schemaName.toUpperCase());
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String functionName = rs.getString("OBJECT_NAME");
                    DatabaseFunction function = new DatabaseFunction(schemaName, functionName);
                    functions.add(function);
                }
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.FUNCTION_NAMES_RETRIEVAL_ERROR, e);
        }

        return functions;
    }
}
