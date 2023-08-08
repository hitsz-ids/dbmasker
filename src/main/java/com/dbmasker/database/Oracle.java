package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Oracle Database class implements the Database interface for Oracle databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a Oracle database.
 */
public class Oracle extends BaseDatabase {

    /**
     * Default constructor for Oracle class.
     */
    public Oracle() {
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
        List<String> schemaNames = new ArrayList<>();
        ResultSet resultSet = null;

        try {
            // Get the metadata object for the database
            DatabaseMetaData metaData = connection.getMetaData();

            // Get the list of schemas
            resultSet = metaData.getSchemas();

            // Iterate through the result set and add the schema names to the list
            while (resultSet.next()) {
                schemaNames.add(resultSet.getString("TABLE_SCHEM"));
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.SCHEMA_NAMES_RETRIEVAL_ERROR, e);
        } finally {
            DbUtils.closeResultSet(resultSet);
        }
        return schemaNames;
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
        if (schemaName != null) {
            schemaName = schemaName.toUpperCase();
        }

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getTables(null, schemaName, "%", new String[] { "TABLE" });

            while (resultSet.next()) {
                tables.add(resultSet.getString("TABLE_NAME"));
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
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<DatabaseFunction> getFuncs(Connection connection, String schemaName) throws SQLException {
        List<DatabaseFunction> funcs = new ArrayList<>();
        String sql;
        if (schemaName == null) {
            sql = "SELECT OWNER, OBJECT_NAME FROM ALL_OBJECTS " +
                    "WHERE OBJECT_TYPE = 'FUNCTION' ";
        } else {
            sql = "SELECT OWNER, OBJECT_NAME FROM ALL_OBJECTS " +
                    "WHERE OBJECT_TYPE = 'FUNCTION' " +
                    "AND OWNER = ?";
        }
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                pstmt.setString(1, schemaName.toUpperCase());
            }
            try (ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    String owner = resultSet.getString("OWNER");
                    String functionName = resultSet.getString("OBJECT_NAME");
                    funcs.add(new DatabaseFunction(owner, functionName));
                }
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.FUNCTION_NAMES_RETRIEVAL_ERROR, e);
        }

        return funcs;
    }

    /**
     * Executes the specified database function with the given parameters.
     *
     * @param connection   The database connection (java.sql.Connection) to execute the function on
     * @param schemaName The name of the schema where the table is located.
     * @param functionName The name of the function to execute
     * @param params       The list of parameters to pass to the function
     * @return A List of Maps containing the function result, where each Map represents a row with column names as keys
     * @throws SQLException if a database access error occurs
     *
     */
    @Override
    public List<Map<String, Object>> executeFunction(Connection connection, String schemaName, String functionName, Object... params) throws SQLException {
        List<Map<String, Object>> result = new ArrayList<>();

        if (schemaName != null && !schemaName.isEmpty()) {
            functionName =  schemaName + "." + functionName;
        }

        // Create a SQL query to execute the specified function
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(functionName).append("(");
        for (int i = 0; i < params.length; i++) {
            sql.append("?");
            if (i < params.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(") AS RESULT FROM DUAL");

        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            // Set the function parameters
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            // Execute the query
            try (ResultSet rs = pstmt.executeQuery()) {
                result = super.getResult(rs, new HashMap<>());
            }
        } catch (Exception e) {
            throw new SQLException(ErrorMessages.FUNCTION_EXECUTION_ERROR + functionName + ".", e);
        }
        return result;
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
        int rowNumStart = (pageOffset - 1) * pageSize + 1;
        int rowNumEnd = rowNumStart + pageSize - 1;
        if (columns.equals("*")) {
            return String.format("SELECT a.* FROM (SELECT t.*, ROWNUM r FROM %s t WHERE ROWNUM <= %d) a WHERE r >= %d", tableName, rowNumEnd, rowNumStart);
        } else {
            return String.format("SELECT %s FROM (SELECT t.*, ROWNUM r FROM %s t WHERE ROWNUM <= %d) WHERE r >= %d", columns, tableName, rowNumEnd, rowNumStart);
        }
    }

    /**
     * Executes the specified SQL query and applies obfuscation rules to the specified columns.
     * @param connection The database connection object.
     * @param schema The name of the schema where the table is located.
     * @param tableName The name of the table or view.
     * @param columnList A list of columns to select. If null or empty, all columns will be selected.
     * @return columnString The list of columns to select.
     */
    @Override
    protected String getColumnString(Connection connection, String schema, String tableName, List<String> columnList) {
        if (columnList == null || columnList.isEmpty()) {
            try {
                return getColumnStrings(connection, schema.toUpperCase(), tableName.toUpperCase());
            } catch (SQLException e) {
                return "*";
            }
        } else {
            return String.join(",", columnList);
        }
    }
}
