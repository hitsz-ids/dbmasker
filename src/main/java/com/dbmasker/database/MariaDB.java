package com.dbmasker.database;

import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.data.TableIndex;
import com.dbmasker.data.TableMetaData;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MariaDB Database class implements the Database interface for MariaDB databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a Hive database.
 */
public class MariaDB extends BaseDatabase {

    /**
     * Default constructor for MariaDB class.
     */
    public MariaDB() {
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
        String query = "SELECT schema_name FROM information_schema.schemata;";

        return DbUtils.getSchemasFromSQL(connection, query);
    }

    /**
     * Retrieves the names of all tables within the specified schema of a database.
     *
     * @param connection the java.sql.Connection object for the database
     * @param schemaName the name of the schema for which to retrieve table names
     * @return a list of table names within the specified schema
     */
    @Override
    public List<String> getTables(Connection connection, String schemaName) throws SQLException {
        List<String> tables = new ArrayList<>();

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getTables(schemaName, null, "%", new String[]{"TABLE"});

            while (resultSet.next()) {
                tables.add(resultSet.getString("TABLE_NAME"));
            }

            resultSet.close();
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.TABLE_NAMES_RETRIEVAL_ERROR, e);
        }

        return tables;
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
        return getViews(connection, schemaName, null);
    }

    /**
     * Retrieves metadata for tables in a given schema.
     *
     * @param connection A valid database connection.
     * @param schemaName The name of the schema for which metadata is to be retrieved.
     * @return A list of TableMetaData objects containing metadata for the tables in the given schema.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<TableMetaData> getMetaData(Connection connection, String schemaName) throws SQLException {
        return getMetaData(connection, schemaName, null);
    }

    /**
     * Retrieves table attributes for a given table in the specified schema.
     *
     * @param connection  A valid database connection.
     * @param schemaName  The schema name where the table is located.
     * @param table       The table for which attributes are to be retrieved.
     * @return A list of TableAttribute objects containing the attributes of the given table.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<TableAttribute> getTableAttribute(Connection connection, String schemaName, String table) throws SQLException {
        return getTableAttribute(connection, schemaName, null, table);
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
        return getUniqueKeys(connection, schemaName, null, table);
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
        return getIndex(connection, schemaName, null, table);
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
        ResultSet resultSet = null;
        String sql;
        if (schemaName == null) {
            sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION'";
        } else {
            sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_SCHEMA = ?";
        }
        // Prepare the SQL query
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)){
            if (schemaName != null) {
                preparedStatement.setString(1, schemaName);
            }
            // Execute the query
            resultSet = preparedStatement.executeQuery();

            // Fetch the results and create DatabaseFunction objects
            while (resultSet.next()) {
                String functionName = resultSet.getString("ROUTINE_NAME");
                DatabaseFunction function = new DatabaseFunction(schemaName, functionName);
                functions.add(function);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.FUNCTION_NAMES_RETRIEVAL_ERROR, e);
        } finally {
            DbUtils.closeResultSet(resultSet);
        }

        return functions;
    }

    /**
     * Helper method to get the column value from a ResultSet.
     *
     * @param rs The ResultSet to be converted (java.sql.ResultSet)
     * @param i The index of the column to get the value from
     * @param columnType The type of the column to get the value from
     * @return The value of the column at the specified index
     * @throws SQLException if a database access error occurs or this method is called on a closed ResultSet
     */
    @Override
    protected Object getColumnValue(ResultSet rs, int i, String columnType) throws SQLException {
        if (columnType.equals("YEAR")) {
            Date date = (Date) rs.getObject(i);
            if (date == null) {
                return null;
            }
            LocalDate localDate = date.toLocalDate();
            return localDate.getYear();
        }
        return rs.getObject(i);
    }
}
