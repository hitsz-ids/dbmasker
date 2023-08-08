package com.dbmasker.database;

import com.dbmasker.data.*;
import com.dbmasker.utils.Config;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import com.dbmasker.utils.ObfuscationUtils;
import net.sf.jsqlparser.JSQLParserException;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * An abstract base class that represents a generic database, providing a foundation for implementing
 * specific database types.
 * <p>
 * This class can be extended by custom classes to implement the required methods and properties for
 * interacting with various types of databases.
 */
public abstract class BaseDatabase implements Database {

    /**
     * Default constructor for BaseDatabase class.
     */
    protected BaseDatabase() {
        super();
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
        return getViews(connection, null, schemaName);
    }

    /**
     * Retrieves the names of all views within the specified schema.
     *
     * @param connection the java.sql.Connection object for the database
     * @param catalog  a catalog name, must match the catalog name as it is stored in the database;
     * @param schemaName the name of the schema for which to retrieve view names
     * @return a list of view names within the specified schema
     * @throws SQLException if a database access error occurs
     */
    public List<String> getViews(Connection connection, String catalog, String schemaName) throws SQLException {
        List<String> views = new ArrayList<>();

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet resultSet = metaData.getTables(catalog, schemaName, "%", new String[]{"VIEW"});){
            // Iterate through the result set and add each view name to the 'views' list
            while (resultSet.next()) {
                views.add(resultSet.getString("TABLE_NAME"));
            }
        }

        // Return the list of view names
        return views;
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
        return getMetaData(connection, null, schemaName);
    }

    /**
     * Retrieves metadata for tables in a given schema.
     *
     * @param connection A valid database connection.
     * @param catalog  a catalog name, must match the catalog name as it is stored in the database;
     * @param schemaName The name of the schema for which metadata is to be retrieved.
     * @return A list of TableMetaData objects containing metadata for the tables in the given schema.
     * @throws SQLException if a database access error occurs
     */
    public static List<TableMetaData> getMetaData(Connection connection, String catalog, String schemaName) throws SQLException {
        List<TableMetaData> metaDataList = new ArrayList<>();

        ResultSet tables = null;
        ResultSet columns = null;
        try {
            DatabaseMetaData dbMetaData = connection.getMetaData();
            tables = dbMetaData.getTables(catalog, schemaName, null, new String[]{"TABLE"});

            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                columns = dbMetaData.getColumns(catalog, schemaName, tableName, null);

                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String dataType = columns.getString("TYPE_NAME");
                    String columnSize = columns.getString("COLUMN_SIZE");

                    TableMetaData metaData = new TableMetaData(tableName, columnName, dataType, columnSize);
                    metaDataList.add(metaData);
                }
            }
        } finally {
            DbUtils.closeResultSet(tables);
            DbUtils.closeResultSet(columns);
        }

        return metaDataList;
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
        return getTableAttribute(connection, null, schemaName, table);
    }

    /**
     * Retrieves the column types of a given table in a database.
     *
     * @param connection The connection to the database.
     * @param schemaName The name of the schema where the table is located.
     * @param table      The name of the table to get column types for.
     * @return           A map where each entry represents a column in the table and its corresponding type.
     *                   The keys in the map are the column names, and the values are the corresponding data types.
     * @throws SQLException If a database access error occurs.
     */
    @Override
    public Map<String, String> getColumnTypes(Connection connection, String schemaName, String table) throws SQLException {
        List<TableAttribute> list = getTableAttribute(connection, schemaName, table);
        Map<String, String> map = new HashMap<>();

        // Iterate through the list of table attributes
        // and populate the map with column names and their corresponding types
        for (TableAttribute attribute : list) {
            map.put(attribute.getName().toLowerCase(), attribute.getTypeName());
        }
        return map;
    }

    /**
     * Retrieves table attributes for a given table in the specified schema.
     *
     * @param connection  A valid database connection.
     * @param catalog  a catalog name， must match the catalog name as it is stored in the database;
     * @param schemaName  The schema name where the table is located.
     * @param table       The table for which attributes are to be retrieved.
     * @return A list of TableAttribute objects containing the attributes of the given table.
     * @throws SQLException if a database access error occurs
     */
    public static List<TableAttribute> getTableAttribute(Connection connection, String catalog, String schemaName, String table) throws SQLException {
        List<TableAttribute> attributes = new ArrayList<>();
        ResultSet resultSet = null;
        ResultSet pkResultSet = null;

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getColumns(catalog, schemaName, table, null);

            Set<String> primaryKeys = new HashSet<>();
            pkResultSet = metaData.getPrimaryKeys(catalog, schemaName, table);
            while (pkResultSet.next()) {
                primaryKeys.add(pkResultSet.getString("COLUMN_NAME"));
            }

            while (resultSet.next()) {
                String name = resultSet.getString("COLUMN_NAME");
                int valueType = resultSet.getInt("DATA_TYPE");
                long maxLength = resultSet.getLong("COLUMN_SIZE");
                boolean required = resultSet.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls;
                boolean autoGenerated = false;
                try {
                    autoGenerated = resultSet.getString("IS_AUTOINCREMENT").equalsIgnoreCase("YES");
                } catch (SQLException | NullPointerException e) {
                    // pass
                }
                int scale = resultSet.getInt("DECIMAL_DIGITS");
                int precision = resultSet.getInt("NUM_PREC_RADIX");
                String typeName = resultSet.getString("TYPE_NAME");
                int ordinalPosition = resultSet.getInt("ORDINAL_POSITION");
                boolean isPrimaryKey = primaryKeys.contains(name);
                TableAttribute attribute = new TableAttribute(name, valueType, maxLength, required, autoGenerated,
                        scale, precision, typeName, ordinalPosition, isPrimaryKey);
                attributes.add(attribute);
            }
        } finally {
            DbUtils.closeResultSet(resultSet);
            DbUtils.closeResultSet(pkResultSet);
        }
        return attributes;
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
        return getPrimaryKeys(connection, null, schemaName, table);
    }

    /**
     * Retrieves the primary keys of a specified table within the given schema.
     *
     * @param connection A valid database connection.
     * @param catalog  a catalog name， must match the catalog name as it is stored in the database;
     * @param schemaName The specified schema name.
     * @param table The specified table name.
     * @return Returns a list of primary key column names for the specified table.
     * @throws SQLException if a database access error occurs
     */
    public List<String> getPrimaryKeys(Connection connection, String catalog, String schemaName, String table) throws SQLException {
        // Get metadata from the connection
        DatabaseMetaData metaData = connection.getMetaData();

        // Get the primary keys for the specified table in the schema
        // If the schema is null, this method retrieves those keys in the default schema
        ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(catalog, schemaName, table);

        // Iterate through the ResultSet and add each primary key to the list
        List<String> primaryKeys = new ArrayList<>();
        while (primaryKeyResultSet.next()) {
            primaryKeys.add(primaryKeyResultSet.getString("COLUMN_NAME"));
        }
        return primaryKeys;
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
        return getUniqueKeys(connection, null, schemaName, table);
    }

    /**
     * Retrieves the unique keys of a specified database table.
     *
     * @param connection A valid database connection.
     * @param catalog  a catalog name， must match the catalog name as it is stored in the database;
     * @param schemaName The specified schema name.
     * @param table The specified table name.
     * @return Returns a Map, where the key is the unique key name, and the value is a collection of unique key column names.
     * @throws SQLException if a database access error occurs
     */
    public Map<String, Set<String>> getUniqueKeys(Connection connection, String catalog, String schemaName, String table) throws SQLException {
        Map<String, Set<String>> uniqueKeys = new HashMap<>();
        ResultSet resultSet = null;

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getIndexInfo(catalog, schemaName, table, true, false);

            while (resultSet.next()) {
                String indexName = resultSet.getString("INDEX_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");

                if (indexName == null || columnName == null) {
                    continue;
                }

                if (uniqueKeys.containsKey(indexName)) {
                    uniqueKeys.get(indexName).add(columnName);
                } else {
                    Set<String> columns = new HashSet<>();
                    columns.add(columnName);
                    uniqueKeys.put(indexName, columns);
                }
            }
        } finally {
            DbUtils.closeResultSet(resultSet);
        }

        return uniqueKeys;
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
        return getIndex(connection, null, schemaName, table);
    }

    /**
     * Retrieves a list of indices for the specified table in the given schema.
     * @param connection A valid database connection.
     * @param catalog  a catalog name， must match the catalog name as it is stored in the database;
     * @param schemaName The name of the schema where the table is located.
     * @param table The name of the table for which to retrieve the indices.
     * @return A list of {@link TableIndex} objects, each containing information about an index in the specified table.
     * @throws SQLException if a database access error occurs
     */
    public static List<TableIndex> getIndex(Connection connection, String catalog, String schemaName, String table) throws SQLException {
        List<TableIndex> indices = new ArrayList<>();
        ResultSet resultSet = null;

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getIndexInfo(catalog, schemaName, table, false, true);

            while (resultSet.next()) {
                String indexName = resultSet.getString("INDEX_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                boolean unique = !resultSet.getBoolean("NON_UNIQUE");
                if (indexName == null || columnName == null) {
                    continue;
                }

                TableIndex index = new TableIndex(indexName, columnName, unique);
                indices.add(index);
            }
        } finally {
            DbUtils.closeResultSet(resultSet);
        }
        return indices;
    }

    /**
     * Executes the given update SQL query using the provided database connection.
     *
     * @param connection The database connection to use for executing the query.
     * @param sql        The SQL query string to execute.
     * @return rowCount  The number of rows affected by the query.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public int executeUpdateSQL(Connection connection, String sql) throws SQLException {
        int rowCount = 0;
        try (Statement statement = connection.createStatement()){
            rowCount = statement.executeUpdate(sql);
        }
        return rowCount;
    }


    /**
     * Executes a batch of update SQL statements and returns the number of successfully updated records.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the batch update
     * @param sqlList The list of update SQL statements to be executed
     * @return rowCount The number of records successfully updated (int)
     * @throws SQLException if a database access error occurs
     */
    @Override
    public int executeUpdateSQLBatch(Connection connection, List<String> sqlList) throws SQLException {
        int rowCount = 0;
        boolean autoCommit = connection.getAutoCommit();

        try (Statement stmt = connection.createStatement()) {
            // Disable auto-commit to allow rollback in case of exceptions
            connection.setAutoCommit(false);

            // Add each SQL statement to the batch
            for (String sql : sqlList) {
                stmt.addBatch(sql);
            }

            // Execute the batch of SQL statements
            int[] updateCounts = stmt.executeBatch();
            // Commit the transaction
            connection.commit();

            // Calculate the number of records successfully updated
            for (int count : updateCounts) {
                if (count != Statement.EXECUTE_FAILED) {
                    rowCount += count;
                }
            }
        } catch (SQLException e) {
            // Rollback the transaction in case of exceptions
            try {
                connection.rollback();
            } catch (SQLException rollbackException) {
                throw new SQLException(ErrorMessages.TRANSACTION_ROLLBACK_ERROR + rollbackException.getMessage());
            }
            throw new SQLException(ErrorMessages.BATCH_UPDATE_SQL_EXECUTION_ERROR + e.getMessage());
        } finally {
            // Recover auto-commit
            connection.setAutoCommit(autoCommit);
        }

        return rowCount;
    }

    /**
     * Executes a SQL query and returns the results as a list of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param sql The SQL query string (String) to be executed
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<Map<String, Object>> executeQuerySQL(Connection connection, String sql) throws SQLException {
        return execQuerySQLWithMask(connection, sql, new HashMap<>());
    }

    /**
     * Executes a SQL query and returns the results as a list of maps and applies obfuscation rules to the specified columns.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @param sql The SQL query string (String) to be executed
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<Map<String, Object>> execQuerySQLWithMask(Connection connection, String sql,
                                                          Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        List<Map<String, Object>> result;
        ResultSet rs = null;

        try (Statement stmt = connection.createStatement()) {
            rs = stmt.executeQuery(sql);
            // Get metadata from the ResultSet to obtain column information
            Map<String, Set<String>> renameMap = new HashMap<>();
            if (Config.getInstance().getHandleRename()) {
                renameMap = DbUtils.getColumnRename(sql);
            }
            result = getResult(rs, obfuscationRules, renameMap);
        } finally {
            DbUtils.closeResultSet(rs);
        }

        // Return the query result as a list of maps
        return result;
    }

    /**
     * Executes a batch of SQL queries and returns the results as a list of lists of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlList The list of SQL query strings to be executed
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<List<Map<String, Object>>> executeQuerySQLBatch(Connection connection, List<String> sqlList) throws SQLException {
        List<List<Map<String, Object>>> batchResult = new ArrayList<>();

        // Use try-with-resources to ensure proper resource management
        try {
            // Iterate through the SQL query list
            for (String sql : sqlList) {
                // Initialize an empty list to store the result of the current query
                List<Map<String, Object>> result = this.executeQuerySQL(connection, sql);

                // Add the result list for the current query to the batchResult list
                batchResult.add(result);
            }
        } catch (SQLException e) {
            throw new SQLException(e);
        }

        // Return the batch query result as a list of lists of maps
        return batchResult;
    }

    /**
     * Executes a SQL query or update statement, returns the results as a list of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param sql The SQL query or update statement to be executed
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<Map<String, Object>> executeSQL(Connection connection, String sql) throws SQLException {
        return executeSQL(connection, sql, new HashMap<>());
    }

    /**
     * Executes a SQL query or update statement, returns the results as a list of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the query
     * @param sql The SQL query or update statement to be executed
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of maps where each map represents a row in the query result,
     *         with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<Map<String, Object>> executeSQL(Connection connection, String sql, Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        List<Map<String, Object>> resultList = new ArrayList<>();
        Map<String, Object> updateResult = new HashMap<>();

        // Try with resources to ensure the statement is closed after use
        try (Statement statement = connection.createStatement()) {
            Locale.setDefault(Locale.ENGLISH);
            // If the SQL is a select statement, execute the query
            if (sql.trim().toLowerCase().startsWith("select")) {
                // Try with resources to ensure the ResultSet is closed after use
                resultList = this.execQuerySQLWithMask(connection, sql, obfuscationRules);
            } else {
                // If the SQL is an update statement, execute the update
                int affectedRows = statement.executeUpdate(sql);
                // Add the number of affected rows to the update result map
                updateResult.put("rows", affectedRows);
                resultList.add(updateResult);
            }
            return resultList;
        } catch (SQLException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Executes a batch of SQL query or update statement, and returns the results as a list of lists of maps.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlList The list of SQL query or update statements to be executed
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<List<Map<String, Object>>> executeSQLBatch(Connection connection, List<String> sqlList) throws SQLException {
        return executeSQLBatch(connection, sqlList, new HashMap<>());
    }

    /**
     * Executes a batch of SQL query or update statement, and returns the results as a list of lists of maps.
     * Applies obfuscation rules to the specified columns.
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
        List<List<Map<String, Object>>> batchResult = new ArrayList<>();
        boolean autoCommit = connection.getAutoCommit();

        // Disable auto-commit to allow rollback in case of exceptions
        connection.setAutoCommit(false);
        // Use try-with-resources to ensure proper resource management
        try {
            // Iterate through the SQL list
            for (String sql : sqlList) {
                // Initialize an empty list to store the result of the current query
                List<Map<String, Object>> result = this.executeSQL(connection, sql, obfuscationRules);

                // Add the result list for the current query to the batchResult list
                batchResult.add(result);
            }
            // Commit the transaction
            connection.commit();

        } catch (SQLException e) {
            // Rollback the transaction
            connection.rollback();

            throw new SQLException(e);
        } finally {
            // Restore the auto-commit mode
            connection.setAutoCommit(autoCommit);
        }

        // Return the batch query result as a list of lists of maps
        return batchResult;
    }

    /**
     * Executes a script of SQL query or update statement, and returns the results as a list of lists of maps.
     * The script is split into individual statements using the semicolon as a delimiter.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlScript The script of SQL query or update statements to be executed, separated by semicolons
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<List<Map<String, Object>>> executeSQLScript(Connection connection, String sqlScript) throws SQLException {
        return executeSQLScript(connection, sqlScript, new HashMap<>());
    }

    /**
     * Executes a script of SQL query or update statement, and returns the results as a list of lists of maps.
     * The script is split into individual statements using the semicolon as a delimiter.
     * Applies obfuscation rules to the specified columns.
     *
     * @param connection The database connection (java.sql.Connection) used to execute the queries
     * @param sqlScript The script of SQL query or update statements to be executed, separated by semicolons
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of lists of maps where each inner list represents a query result,
     *         with each map representing a row in the result set, with keys being column names and values being the corresponding data.
     *         For an update statement, the list contains a single map with a key "rows"
     *         and a value representing the number of affected rows.
     * @throws SQLException if a database access error occurs
     */
    @Override
    public List<List<Map<String, Object>>> executeSQLScript(Connection connection, String sqlScript, Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        // Split the script into individual statements using the semicolon as a delimiter
        List<String> sqlList;
        try {
            sqlList = DbUtils.splitSqlScript(sqlScript);
        } catch (JSQLParserException e) {
            sqlList = splitSqlScript(sqlScript, ";");
        }

        if (sqlList == null) {
            throw new IllegalArgumentException(ErrorMessages.NULL_SQL_LIST_ERROR);
        }
        // Execute the SQL statements in the script
        return executeSQLBatch(connection, sqlList, obfuscationRules);
    }

    /**
     * Commits a transaction for the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) on which the transaction is being executed
     * @return true if the transaction was successfully committed, false otherwise
     * @throws SQLException if a database access error occurs
     */
    @Override
    public boolean commit(Connection connection) throws SQLException {
        // Ensure the connection is not in auto-commit mode
        if (!connection.getAutoCommit())
            // Commit the transaction
            connection.commit();

        return true;
    }

    /**
     * Sets the auto-commit mode for the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) for which the auto-commit mode should be set
     * @param autoCommit The auto-commit mode (boolean) to set for the connection
     * @return true if the auto-commit mode was successfully set, false otherwise
     * @throws SQLException if a database access error occurs
     */
    @Override
    public boolean setAutoCommit(Connection connection, boolean autoCommit) throws SQLException {
        // Set the auto-commit mode for the connection
        connection.setAutoCommit(autoCommit);
        return true;
    }

    /**
     * Rolls back a transaction for the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) on which the transaction is being executed
     * @return true if the transaction was successfully rolled back, false otherwise
     * @throws SQLException if a database access error occurs
     */
    @Override
    public boolean rollback(Connection connection) throws SQLException {
        boolean isSuccess = false;

        // Ensure the connection is not in auto-commit mode
        if (!connection.getAutoCommit()) {
            // Roll back the transaction
            connection.rollback();
            isSuccess = true;
        }

        return isSuccess;
    }

    /**
     * Fetches data from the specified table or view in the given database connection.
     *
     * @param connection The database connection (java.sql.Connection) to fetch the data from
     * @param schemaName The name of the schema where the table is located.
     * @param name  The name of the table/view to fetch the data from
     * @return A List of Maps containing the table/view data, where each Map represents a row with column names as keys and column values as values
     * @throws SQLException If an error occurs while fetching the table/view data
     */
    @Override
    public List<Map<String, Object>> getTableOrViewData(Connection connection, String schemaName, String name) throws SQLException {
        return this.getDataWithMask(connection, schemaName, name, new HashMap<>());
    }

    /**
     * Fetches table or view data from the database and applies obfuscation rules to the specified columns.
     *
     * @param connection      The database connection object.
     * @param name            The name of the table or view.
     * @param schemaName The name of the schema where the table is located.
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A list of maps representing the rows of the table or view with the specified obfuscation rules applied.
     * @throws SQLException If an error occurs while fetching the table or view data.
     */
    @Override
    public List<Map<String, Object>> getDataWithMask(Connection connection, String schemaName, String name,
                                                     Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        String sql = "SELECT * FROM " + name;
        if (schemaName != null && !schemaName.isEmpty()) {
            sql = "SELECT * FROM " + schemaName + "." + name;
        }

        return this.execQuerySQLWithMask(connection, sql, obfuscationRules);
    }

    /**
     * This function retrieves data from a table or view in the database. It supports both pagination and selection of specific columns.
     *
     * @param connection The database connection object.
     * @param schemaName The schema name of the table or view.
     * @param tableName The name of the table or view.
     * @param columnList A list of columns to select. If null or empty, all columns will be selected.
     * @param pageOffset The offset for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @param pageSize The size of a page for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @return A Map object containing the retrieved data in 'results' and the total pages of data in 'totalPages'.
     * @throws SQLException If a database access error occurs.
     */
    @Override
    public Map<String, Object> getDataWithPage(Connection connection, String schemaName, String tableName,
                                               List<String> columnList, int pageOffset, int pageSize) throws SQLException {
        return getDataWithPage(connection, schemaName, tableName, columnList, pageOffset, pageSize, new HashMap<>());
    }

    /**
     * This function retrieves data from a table or view in the database. It supports both pagination and selection of specific columns.
     *
     * @param connection The database connection object.
     * @param schemaName The schema name of the table or view.
     * @param tableName The name of the table or view.
     * @param columnList A list of columns to select. If null or empty, all columns will be selected.
     * @param pageOffset The offset for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @param pageSize The size of a page for pagination. If less than or equal to 0, all data will be returned without pagination.
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A Map object containing the retrieved data in 'results' and the total pages of data in 'totalPages'.
     * @throws SQLException If a database access error occurs.
     */
    @Override
    public Map<String, Object> getDataWithPage(Connection connection, String schemaName, String tableName,
                                               List<String> columnList, int pageOffset, int pageSize,
                                               Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        List<Map<String, Object>> resultList;
        int totalRecord = 0;
        int totalPages = 1;

        // if columnList is null or empty, get all columns
        String columns = getColumnString(connection, schemaName, tableName, columnList);
        String query;

        if (schemaName != null && !schemaName.isEmpty()) {
            tableName = schemaName + "." + tableName;
        }

        if (pageOffset <= 0 || pageSize <= 0) {
            // if pageOffset and pageSize are less than or equal to 0, return all data
            query = String.format("SELECT %s FROM %s", columns, tableName);
        } else {
            // get total record
            String countQuery = String.format("SELECT COUNT(*) AS total FROM %s", tableName);
            try (Statement countStatement = connection.createStatement();
                 ResultSet countResultSet = countStatement.executeQuery(countQuery)) {
                if (countResultSet.next()) {
                    totalRecord = countResultSet.getInt("total");
                }
            } catch (SQLException e) {
                throw new SQLException(e);
            }

            // calculate total pages
            totalPages = totalRecord / pageSize;
            if (totalRecord % pageSize != 0) {
                totalPages++;
            }

            query = this.getQueryWithPage(columns, tableName, pageSize, pageOffset);
        }

        resultList = execQuerySQLWithMask(connection, query, obfuscationRules);

        Map<String, Object> result = new HashMap<>();
        result.put("results", resultList);
        result.put("totalPages", totalPages);
        return result;
    }

    /**
     * Executes the specified SQL query and applies obfuscation rules to the specified columns.
     * @param connection The database connection object.
     * @param schema The name of the schema where the table is located.
     * @param tableName The name of the table or view.
     * @param columnList A list of columns to select. If null or empty, all columns will be selected.
     * @return columnString The list of columns to select.
     */
    protected String getColumnString(Connection connection, String schema, String tableName, List<String> columnList) {
        return columnList == null || columnList.isEmpty() ? "*" : String.join(",", columnList);
    }

    /**
     * This method fetches all the column names for a given table or view in the Oracle database.
     *
     * @param connection The connection object representing a connection with a specific database.
     * @param schema     The name of the schema where the table is located.
     * @param tableName  The name of the table or view from which to fetch the column names.
     * @return A comma-separated string of all column names in the given table or view.
     * @throws SQLException If a database access error occurs or this method is called on a closed connection.
     */
    protected String getColumnStrings(Connection connection, String schema, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();

        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet resultSet = metaData.getColumns(null, schema, tableName, "%")) {
            while (resultSet.next()) {
                columns.add(resultSet.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            throw new SQLException(e);
        }
        return String.join(",", columns);
    }

    /**
     * Executes the specified SQL query and applies obfuscation rules to the specified columns.
     * @param columns The columns to fetch from the table/view.
     * @param tableName The name of the table/view to fetch the data from.
     * @param pageSize The number of records to fetch.
     * @param pageOffset The offset of the first record to fetch.
     * @return The SQL query to execute.
     */
    protected String getQueryWithPage(String columns, String tableName, int pageSize, int pageOffset) {
        int offset = (pageOffset - 1) * pageSize;
        return String.format("SELECT %s FROM %s LIMIT %d OFFSET %d", columns, tableName, pageSize, offset);
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
     */
    @Override
    public List<Map<String, Object>> executeFunction(Connection connection, String schemaName, String functionName,
                                                     Object... params) throws SQLException {
        return executeFunction(connection, "SELECT ", schemaName, functionName, params);
    }

    /**
     * Executes the specified database function with the given parameters.
     *
     * @param connection   The database connection (java.sql.Connection) to execute the function on
     * @param startSql   The start keywords used to execute function, like: SELECT, EXECUTE FUNCTION.
     * @param schemaName The name of the schema where the table is located.
     * @param functionName The name of the function to execute
     * @param params       The list of parameters to pass to the function
     * @return A List of Maps containing the function result, where each Map represents a row with column names as keys
     * @throws SQLException if a database access error occurs
     */
    protected List<Map<String, Object>> executeFunction(Connection connection, String startSql, String schemaName,
                                                        String functionName, Object... params) throws SQLException {
        List<Map<String, Object>> result = new ArrayList<>();

        if (schemaName != null && !schemaName.isEmpty()) {
            functionName =  schemaName + "." + functionName;
        }

        // Create a SQL query to execute the specified function
        StringBuilder sql = new StringBuilder(startSql);
        sql.append(functionName).append("(");
        for (int i = 0; i < params.length; i++) {
            sql.append("?");
            if (i < params.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(")");

        try (PreparedStatement pstmt = connection.prepareStatement(sql.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            // Set the function parameters
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            // Execute the query
            try (ResultSet rs = pstmt.executeQuery()) {
                result = getResult(rs, new HashMap<>());
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.FUNCTION_EXECUTION_ERROR + e.getMessage(), e);
        }

        return result;
    }


    /**
     * Scans a database table or view for sensitive data based on a list of regular expressions.
     *
     * @param connection The SQL connection to the database.
     * @param schemaName The name of the schema where the table is located.
     * @param tableName  The name of the table or view to scan.
     * @param regexList  A list of regular expressions used for matching sensitive data.
     * @return A list of {@link SensitiveColumn} instances containing matched sensitive data.
     *         Only the columns that actually matched sensitive data will be returned.
     *         For each SensitiveColumn, up to 5 matched data will be stored in the matchData attribute.
     * @throws SQLException if a database access error occurs.
     */
    @Override
    public List<SensitiveColumn> scanTableData(Connection connection, String schemaName, String tableName, List<String> regexList) throws SQLException {
        List<SensitiveColumn> sensitiveColumns = new ArrayList<>();

        String sql = "SELECT * FROM " + tableName;
        if (schemaName != null && !schemaName.isEmpty()) {
            sql = "SELECT * FROM " + schemaName + "." + tableName;
        }

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Initialize SensitiveColumn instances
            for (String regex : regexList) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    SensitiveColumn sensitiveColumn = new SensitiveColumn(schemaName, tableName, columnName, regex);
                    sensitiveColumns.add(sensitiveColumn);
                }
            }

            // Scan data and fill matchData in SensitiveColumn instances
            while (rs.next()) {
                for (SensitiveColumn sensitiveColumn : sensitiveColumns) {
                    String columnName = sensitiveColumn.getColumnName();
                    String regex = sensitiveColumn.getRegex();
                    Object columnValue = rs.getObject(columnName);

                    Pattern pattern = Pattern.compile(regex);
                    Matcher matcher = pattern.matcher(columnValue.toString());
                    if (sensitiveColumn.getMatchData().size() < Config.getInstance().getDataSize() && matcher.find()) {
                        sensitiveColumn.getMatchData().add(columnValue);
                    }
                }
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.TABLE_SCANNING_ERROR + e.getMessage(), e);
        }

        List<SensitiveColumn> resultSensitiveColumns = new ArrayList<>();
        for (SensitiveColumn column : sensitiveColumns) {
            if (!column.getMatchData().isEmpty()) {
                resultSensitiveColumns.add(column);
            }
        }
        return resultSensitiveColumns;
    }

    /**
     * Helper method to convert a ResultSet into a List of Maps and applies obfuscation rules to the specified columns.
     * Each Map in the List represents a row in the ResultSet, with column names as keys.
     *
     * @param rs The ResultSet to be converted (java.sql.ResultSet)
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @return A List of Maps containing the data from the ResultSet
     * @throws SQLException if a database access error occurs or this method is called on a closed ResultSet
     */
    protected List<Map<String, Object>> getResult(ResultSet rs, Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        return getResult(rs, obfuscationRules, new HashMap<>());
    }

    /**
     * Helper method to convert a ResultSet into a List of Maps and applies obfuscation rules to the specified columns.
     * Each Map in the List represents a row in the ResultSet, with column names as keys.
     *
     * @param rs The ResultSet to be converted (java.sql.ResultSet)
     * @param obfuscationRules A map where the key is the column name and the value is the ObfuscationRule object to apply to that column.
     * @param renameMap a map containing column rename rules, where the key is the original column name and the value is the renamed column
     * @return A List of Maps containing the data from the ResultSet
     * @throws SQLException if a database access error occurs or this method is called on a closed ResultSet
     */
    protected List<Map<String, Object>> getResult(ResultSet rs, Map<String, ObfuscationRule> obfuscationRules, Map<String, Set<String>> renameMap) throws SQLException {
        List<Map<String, Object>> result = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Iterate through the result set and build the list of maps
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                String columnType;
                try {
                    columnType = metaData.getColumnTypeName(i);
                } catch (SQLException e) {
                    columnType = null;
                }
                Object columnValue = getColumnValue(rs, i, columnType);

                for (Map.Entry<String, ObfuscationRule> entry: obfuscationRules.entrySet()) {
                    if (DbUtils.columnMatch(columnName, entry.getKey(), renameMap)){
                        ObfuscationRule rule = entry.getValue();
                        columnValue = ObfuscationUtils.doObfuscation(columnValue, rule);
                    }
                }
                row.put(columnName, columnValue);
            }
            result.add(row);
        }
        return result;
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
    protected Object getColumnValue(ResultSet rs, int i, String columnType) throws SQLException {
        return rs.getObject(i);
    }

    /**
     * Executes a batch of SQL query or update statement, and returns the results as a list of lists of maps.
     * This method does not use transactions.
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
    protected List<List<Map<String, Object>>> executeSQLBatchNoTransaction(Connection connection, List<String> sqlList, Map<String, ObfuscationRule> obfuscationRules) throws SQLException {
        List<List<Map<String, Object>>> batchResult = new ArrayList<>();
        // Use try-with-resources to ensure proper resource management
        try {
            // Iterate through the SQL list
            for (String sql : sqlList) {
                // Initialize an empty list to store the result of the current query
                List<Map<String, Object>> result = this.executeSQL(connection, sql, obfuscationRules);

                // Add the result list for the current query to the batchResult list
                batchResult.add(result);
            }
        } catch (SQLException e) {
            throw new SQLException(e);
        }
        // Return the batch query result as a list of lists of maps
        return batchResult;
    }

    /**
     * Splits a SQL script into individual statements using the specified delimiter.
     *
     * @param sqlScript The SQL script to be split
     * @param delimiter The delimiter used to split the SQL script
     * @return A list of SQL statements
     */
    private List<String> splitSqlScript(String sqlScript, String delimiter) {
        return Arrays.stream(sqlScript.split(delimiter))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}
