package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * KingBase Database class implements the Database interface for KingBase databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a Hive database.
 */
public class KingBase extends BaseDatabase {

    /**
     * Default constructor for KingBase class.
     */
    public KingBase() {
        super();
    }

    /**
     * Get a list of schema names for a Kingbase database.
     *
     * @param connection The database connection to be used.
     * @return A list of schema names in the connected Kingbase database.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<String> getSchemas(Connection connection) throws SQLException {
        // SQL query to get schema names from the information_schema.schemata view
        String sql = "SELECT schema_name FROM information_schema.schemata";

        return DbUtils.getSchemasFromSQL(connection, sql);
    }

    /**
     * Retrieves the tables in a database for the given schema.
     *
     * @param connection A java.sql.Connection object representing the connection to the database.
     * @param schemaName A String representing the name of the schema to query.
     * @return A List String containing the names of all tables in the database.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<String> getTables(Connection connection, String schemaName) throws SQLException {
        List<String> tables = new ArrayList<>();
        ResultSet rs = null;
        String sql;

        if (schemaName == null) {
            sql = "SELECT tablename FROM pg_tables";
        } else {
            sql = "SELECT tablename FROM pg_tables WHERE schemaname = ?";
        }
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                pstmt.setString(1, schemaName);
            }
            rs = pstmt.executeQuery();

            while (rs.next()) {
                String tableName = rs.getString("tablename");
                tables.add(tableName);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.TABLE_NAMES_RETRIEVAL_ERROR, e);
        } finally {
            DbUtils.closeResultSet(rs);
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
        List<DatabaseFunction> functions = new ArrayList<>();
        ResultSet rs = null;
        String sql;
        if (schemaName == null) {
            sql = "SELECT nspname, proname, typname " +
                    "FROM pg_proc " +
                    "JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid " +
                    "JOIN pg_type ON pg_proc.prorettype = pg_type.oid ";
        } else {
            sql = "SELECT nspname, proname, typname " +
                    "FROM pg_proc " +
                    "JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid " +
                    "JOIN pg_type ON pg_proc.prorettype = pg_type.oid " +
                    "WHERE nspname = ?;";
        }
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            if (schemaName != null) {
                pstmt.setString(1, schemaName);
            }
            rs = pstmt.executeQuery();

            while (rs.next()) {
                String functionName = rs.getString("proname");
                functions.add(new DatabaseFunction(schemaName, functionName));
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.FUNCTION_NAMES_RETRIEVAL_ERROR, e);
        } finally {
            DbUtils.closeResultSet(rs);
        }

        return functions;
    }
}
