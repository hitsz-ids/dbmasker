package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * SQLiteDatabase class implements the Database interface for SQLite databases.
 * Provides an implementation to retrieve a list of schemas (tables) from an SQLite database.
 */
public class SQLite extends BaseDatabase {

    /**
     * Default constructor for SQLiteDatabase class.
     */
    public SQLite() {
        super();
    }

    /**
     * Retrieves a list of schema (table) names from the given SQLite database connection.
     *
     * @param connection A java.sql.Connection object representing the SQLite database connection.
     * @return A list of schema (table) names as strings.
     * @throws SQLFeatureNotSupportedException if the database does not support this method
     */
    @Override
    public List<String> getSchemas(Connection connection) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("SQLite does not support schemas");
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

        String query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_sequence'";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            // Iterate through the result set and add each table name to the 'tables' list
            while (resultSet.next()) {
                tables.add(resultSet.getString("name"));
            }
        } catch (Exception e) {
            // If an exception occurs, print the stack trace
            throw new SQLException(ErrorMessages.TABLE_NAMES_RETRIEVAL_ERROR, e);
        }

        // Return the list of table names
        return tables;
    }

    /**
     * Retrieves a list of functions in the specified schema.(not supported yet)
     *
     * @param connection A valid database connection.
     * @param schemaName The name of the specified schema.
     * @return Returns a list of DatabaseFunction objects, containing all the functions in the given schema.
     * @throws SQLFeatureNotSupportedException if the database does not support this method
     */
    @Override
    public List<DatabaseFunction> getFuncs(Connection connection, String schemaName) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("SQLite does not support functions");
    }

}
