package com.dbmasker.dialect;


import com.dbmasker.database.Database;
import com.dbmasker.database.DatabaseFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * BaseDialect class implements the Dialect interface.
 */
public abstract class BaseDialect implements Dialect {

    /**
     * Default constructor for BaseDialect class.
     */
    protected BaseDialect() {
        super();
    }

    /**
     * Generates a SQL INSERT statement for the given data.
     *
     * @param connection The connection to the database.
     * @param schemaName  The name of the schema where the table is located. If null or empty, the schema name is not included in the SQL.
     * @param tableName   The name of the table where the data should be inserted.
     * @param data        A map containing the data to be inserted. Each entry in the map represents a column and its corresponding value.
     * @param dbType      The type of the database (e.g., "sqlite", "mysql", etc.).
     * @return A string representing the SQL INSERT statement.
     * @throws IllegalArgumentException If the provided data is empty.
     */
    @Override
    public String generateInsertSql(Connection connection, String schemaName, String tableName, Map<String, Object> data, String dbType) throws SQLException {
        Database database = new DatabaseFactory().getDatabase(dbType);
        Map<String, String> columnTypes = database.getColumnTypes(connection, schemaName, tableName);
        return generateInsertSql(schemaName, tableName, data, columnTypes);
    }

    /**
     * Generates a SQL INSERT statement for the given data.
     *
     * @param schemaName  The name of the schema where the table is located. If null or empty, the schema name is not included in the SQL.
     * @param tableName   The name of the table where the data should be inserted.
     * @param data        A map containing the data to be inserted. Each entry in the map represents a column and its corresponding value.
     * @param columnTypes A map containing the data types of each column. Each entry in the map represents a column and its corresponding data type.
     * @return A string representing the SQL INSERT statement.
     * @throws IllegalArgumentException If the provided data is empty.
     */
    @Override
    public String generateInsertSql(String schemaName, String tableName, Map<String, Object> data, Map<String, String> columnTypes) {
        StringBuilder sql = sqlInit(schemaName, data);
        sql.append(tableName).append(" (");

        // Append column names
        for (String column : data.keySet()) {
            sql.append(column).append(",");
        }

        // Replace the last comma with a closing parenthesis
        sql.setCharAt(sql.length() - 1, ')');

        sql.append(" VALUES\n(");

        // Append values
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String column = entry.getKey().toLowerCase();
            Object value = entry.getValue();
            String type = columnTypes.get(column);
            sql.append(formatData(value, type)).append(",");
        }

        // Replace the last comma with a closing parenthesis
        sql.setCharAt(sql.length() - 1, ')');

        sql.append(";");

        return sql.toString();
    }

    /**
     * This method initializes a StringBuilder for SQL generation, starting with "INSERT INTO"
     * and optionally prepending a schema name. If the data provided is empty,
     * it throws an IllegalArgumentException.
     *
     * @param schemaName The name of the schema. This is optional and can be null or empty.
     * @param data The data to be inserted. This is used to check if the data provided is empty.
     * @return A StringBuilder initialized with the "INSERT INTO" SQL command and the schema name if provided.
     * @throws IllegalArgumentException if the data provided is empty.
     */
    protected StringBuilder sqlInit(String schemaName, Map<String, Object> data) {
        if (data.isEmpty()) {
            throw new IllegalArgumentException("No data provided to generate SQL");
        }

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        if (schemaName != null && !schemaName.trim().isEmpty()) {
            sql.append(schemaName).append(".");
        }
        return sql;
    }

    /**
     * Generates an SQL UPDATE statement for the specified table with the provided conditions.
     *
     * @param connection The active database connection.
     * @param dbType The type of the database (e.g., "MySQL", "PostgreSQL").
     * @param schemaName The name of the schema.
     * @param tableName The name of the table for which the SQL update statement is to be generated.
     * @param setData The key-value pairs representing columns and their corresponding values to be updated.
     * @param condition The conditions that determine which rows will be updated.
     * @param filteredByUniqueKey Flag indicating whether to filter the conditions by unique keys.
     * @return A string representing the SQL UPDATE statement.
     * @throws SQLException If any SQL-related error occurs.
     */
    @Override
    public String generateUpdateSql(Connection connection, String dbType, String schemaName, String tableName,
                               Map<String, Object> setData,
                               Map<String, Object> condition,
                               boolean filteredByUniqueKey) throws SQLException {
        Database database = new DatabaseFactory().getDatabase(dbType);
        Map<String, String> columnTypes = database.getColumnTypes(connection, schemaName, tableName);
        // Return null if either setData or condition is empty
        if (setData == null || setData.isEmpty() || condition == null || condition.isEmpty()) {
            return null;
        }

        // If the filteredByUniqueKey flag is set to true, filter the conditions by unique keys.
        if (filteredByUniqueKey) {
            condition = getFilteredCondition(connection, database, schemaName, tableName, condition);
        }

        // Generate and return the SQL UPDATE statement using the given parameters.
        return generateUpdateSql(schemaName, tableName, setData, condition, columnTypes);
    }

    /**
     * Generates an SQL UPDATE statement based on provided schema, table, data sets, conditions, and column types.
     *
     * @param schemaName  The name of the database schema.
     * @param tableName   The name of the table within the schema.
     * @param setData     A map containing the columns and values that need to be updated.
     * @param condition   A map containing the conditions for which rows should be updated.
     * @param columnTypes A map specifying the data type of each column.
     * @return An SQL UPDATE statement in string format.
     */
    @Override
    public String generateUpdateSql(String schemaName, String tableName,
                               Map<String, Object> setData,
                               Map<String, Object> condition,
                               Map<String, String> columnTypes) {
        StringBuilder sql = new StringBuilder("UPDATE ");
        if (schemaName != null && !schemaName.trim().isEmpty()) {
            sql.append(schemaName).append(".");
        }
        sql.append(tableName).append(" SET ");

        // Return null if either setData or condition is empty
        if (setData.isEmpty() || condition.isEmpty()) {
            return null;
        }

        // Construct the SET part of the SQL query
        for (Map.Entry<String, Object> entry : setData.entrySet()) {
            String column = entry.getKey().toLowerCase();
            Object data = entry.getValue();
            String type = columnTypes.get(column);
            String formattedData = formatData(data, type);

            sql.append(column).append(" = ").append(formattedData).append(", ");
        }

        // Remove the trailing comma after the last SET column-value pair
        sql.deleteCharAt(sql.length() - 2);

        sql.append(" WHERE ");

        // Construct the WHERE part of the SQL query
        sql.append(constructWhereClause(condition, columnTypes));

        sql.append(";");

        return sql.toString();
    }

    /**
     * Generates a SQL DELETE statement for the given table and conditions.
     *
     * @param connection          The database connection.
     * @param dbType              The type of the database.
     * @param schemaName          The name of the schema.
     * @param tableName           The name of the table.
     * @param condition           A map representing the conditions for the DELETE operation.
     * @param filteredByUniqueKey If true, the conditions are filtered by the unique keys.
     * @return The SQL DELETE statement as a string.
     * @throws SQLException If any SQL related error occurs.
     */
    public String generateDeleteSql(Connection connection, String dbType, String schemaName, String tableName,
                                    Map<String, Object> condition, boolean filteredByUniqueKey) throws SQLException {
        Database database = new DatabaseFactory().getDatabase(dbType);
        Map<String, String> columnTypes = database.getColumnTypes(connection, schemaName, tableName);
        // Return null if either setData or condition is empty
        if (condition == null || condition.isEmpty()) {
            return null;
        }

        // If the filteredByUniqueKey flag is set to true, filter the conditions by unique keys.
        if (filteredByUniqueKey) {
            condition = getFilteredCondition(connection, database, schemaName, tableName, condition);
        }

        StringBuilder sql = new StringBuilder("DELETE FROM ");
        if (schemaName != null && !schemaName.trim().isEmpty()) {
            sql.append(schemaName).append(".");
        }

        sql.append(tableName)
                .append(" WHERE ")
                .append(constructWhereClause(condition, columnTypes))
                .append(";");

        return sql.toString();
    }

    /**
     * Constructs the WHERE clause of an SQL query based on the given condition and column types.
     *
     * @param condition A map representing the condition columns and their respective values.
     * @param columnTypes A map representing the column types for the columns in the condition.
     * @return A string representing the WHERE clause of an SQL query.
     */
    protected String constructWhereClause(Map<String, Object> condition, Map<String, String> columnTypes) {
        StringBuilder whereClause = new StringBuilder();

        // Iterate through each entry in the condition map
        for (Map.Entry<String, Object> entry : condition.entrySet()) {
            String column = entry.getKey().toLowerCase();
            Object data = entry.getValue();
            String type = columnTypes.get(column);
            String formattedData = formatData(data, type);

            // Check if the value is NULL and handle accordingly
            if (data == null || "NULL".equalsIgnoreCase(formattedData)) {
                whereClause.append(column).append(" IS NULL AND ");
            } else {
                whereClause.append(column).append(" = ").append(formattedData).append(" AND ");
            }
        }

        // Remove the trailing "AND" and return the constructed where clause
        return whereClause.substring(0, whereClause.length() - 5);
    }

    /**
     * Filters the given condition map based on unique keys, primarily using primary keys.
     * If the condition remains unchanged after filtering with primary keys, other unique keys are used.
     *
     * @param connection The database connection.
     * @param database The database abstraction object.
     * @param schemaName The name of the schema.
     * @param tableName The name of the table.
     * @param originalCondition The original condition map.
     * @return A filtered condition map based on unique keys.
     * @throws SQLException If any SQL-related error occurs.
     */
    protected Map<String, Object> getFilteredCondition(Connection connection, Database database, String schemaName,
                                                       String tableName, Map<String, Object> originalCondition) throws SQLException {
        Set<Set<String>> uniqueKeys = new HashSet<>();

        // Add primary keys to uniqueKeys set
        Set<String> primaryKeys = database.getPrimaryKeys(connection, schemaName, tableName);
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            uniqueKeys.add(primaryKeys);
        }

        Map<String, Object> newCondition = conditionFilter(originalCondition, uniqueKeys);

        // If condition size remains unchanged after filtering with primary keys, then filter with other unique keys.
        if (newCondition.size() == originalCondition.size()) {
            Map<String, Set<String>> uniqueKeyMap = database.getUniqueKeys(connection, schemaName, tableName);
            uniqueKeys.addAll(uniqueKeyMap.values());
            newCondition = conditionFilter(originalCondition, uniqueKeys);
        }

        return newCondition;
    }

    /**
     * Filters the given condition map to retain only the key-value pairs that form a complete unique key
     * from the provided set of unique keys.
     *
     * @param condition The original conditions as a map of column names to values.
     * @param uniqueKeys A set containing sets of column names that form unique keys.
     * @return A filtered map containing only the key-value pairs that form a complete unique key, or the original condition
     * map if no complete unique key is present.
     */
    protected Map<String, Object> conditionFilter(Map<String, Object> condition, Set<Set<String>> uniqueKeys) {
        // Map to store the key-value pairs from the condition that match a unique key
        Map<String, Object> result = new HashMap<>();

        // Convert the keys in the condition map to lowercase
        Map<String, Object> conditionLower = new HashMap<>();
        for (Map.Entry<String, Object> entry : condition.entrySet()) {
            conditionLower.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        // Iterate over each unique key
        for (Set<String> uniqueKey : uniqueKeys) {
            if (uniqueKey == null || uniqueKey.isEmpty()) {
                continue;
            }
            boolean match = true;

            // Check if all columns in the current unique key are present in the condition
            for (String key : uniqueKey) {
                if (!conditionLower.containsKey(key.toLowerCase())) {
                    match = false;
                    break;
                }
            }

            // If a complete unique key is found in the condition, add its key-value pairs to the result map
            if (match) {
                for (String key : uniqueKey) {
                    result.put(key, conditionLower.get(key.toLowerCase()));
                }
                return result;
            }
        }

        // If no complete unique key is found, return the original condition map
        return condition;
    }

}
