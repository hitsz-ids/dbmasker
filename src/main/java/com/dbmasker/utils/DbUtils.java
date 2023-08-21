package com.dbmasker.utils;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.*;

/**
 * A utility class that provides helper methods for database-related operations.
 */
public class DbUtils {

    /**
     * Default constructor for DbUtils class.
     */
    private DbUtils() {
    }

    /**
     * Closes a ResultSet object, releasing its associated resources.
     * @param rs The ResultSet object to be closed.
     */
    public static void closeResultSet(ResultSet rs){
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // pass
            }
        }
    }

    /**
     * Closes a Statement object, releasing its associated resources.
     * @param stmt The Statement object to be closed.
     * @throws SQLException If an error occurs while closing the Statement.
     */
    public static void closeStatement(Statement stmt) throws SQLException {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                throw new SQLException("Unable to close Statement", e);
            }
        }
    }

    /**
     * Retrieves a list of strings from the result of executing a SQL query.
     * @param connection The Connection object to the database.
     * @param query The SQL query to be executed for retrieving the strings.
     * @return A list of schema strings containing the retrieved strings from the SQL query.
     * @throws SQLException If an error occurs while executing the SQL query or processing the ResultSet.
     */
    public static List<String> getSchemasFromSQL(Connection connection, String query) throws SQLException {
        List<String> schemas = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String schemaName = resultSet.getString("schema_name");
                schemas.add(schemaName);
            }
        } catch (SQLException e) {
            throw new SQLException(ErrorMessages.SCHEMA_NAMES_RETRIEVAL_ERROR, e);
        }

        return schemas;
    }

    /**

     This method is used to retrieve a list of results from a Map.
     @param result A Map object containing a 'results' key, where the value should be A list of maps.
     @return Returns a list of results. If the value of 'results' is not of this type, an empty list will be returned.
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> getResultList(Map<String, Object> result) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        Object obj = result.get("results");
        try {
            if (obj instanceof List) {
                resultList = (List<Map<String, Object>>) obj;
            }
        } catch (ClassCastException e) {
            // pass
        }
        return resultList;
    }

    /**
     * Parses the provided SQL query and extracts the original column names and their aliases (new names).
     *
     * @param sql The SQL query as a String.
     * @return A Map where the key is the alias (new name) and their corresponding original column names are added.
     */
    public static Map<String, Set<String>> getColumnRename(String sql) {
        Map<String, Set<String>> renameMap = new HashMap<>();

        // Parse the SQL query using JSQLParser.
        Select selectStatement = null;
        try {
            selectStatement = (Select) CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException | ClassCastException e) {
            // If an error occurs while parsing the SQL query, return an empty map.
            return renameMap;
        }

        // Extract the body of the SELECT statement.
        SelectBody selectBody = selectStatement.getSelectBody();

        // Call the recursive method to handle the SelectBody.
        handleSelectBody(selectBody, renameMap);

        return transform(renameMap);
    }

    /**
     * Recursive method to handle each SelectBody in the SQL query. If the SelectBody is a PlainSelect,
     * it processes its SelectItems and checks for any alias. If an alias is found, it is added to the
     * provided map along with the original column names. This method also handles SubSelects in the
     * FROM clause and recursively processes them. If the SelectBody is a SetOperationList, this method
     * recursively processes each SelectBody in the operation.
     *
     * @param selectBody The SelectBody of the SQL query to process.
     * @param renameMap The map to which any found aliases and their corresponding original column names are added.
     */
    private static void handleSelectBody(SelectBody selectBody, Map<String, Set<String>> renameMap) {
        if (selectBody instanceof PlainSelect plainSelect) {
            List<SelectItem> selectItems = plainSelect.getSelectItems();

            // Iterate over each item.
            for (SelectItem selectItem : selectItems) {
                // If the item is a SelectExpressionItem, it can have an alias.
                if (selectItem instanceof SelectExpressionItem selectExpressionItem) {
                    // Get the alias of the select item (if it exists).
                    Alias alias = selectExpressionItem.getAlias();

                    // If an alias exists, add the original column name to the set associated with this alias in the map.
                    if (alias != null) {
                        String originalColumnName = processString(selectExpressionItem.getExpression().toString().toLowerCase());
                        String aliasName = processString(alias.getName().toLowerCase());
                        renameMap.computeIfAbsent(aliasName, k -> new HashSet<>()).add(originalColumnName);
                    }
                }
            }
            // handle sub-select in the FROM clause
            FromItem fromItem = plainSelect.getFromItem();
            if (fromItem instanceof SubSelect subSelect) {
                handleSelectBody(subSelect.getSelectBody(), renameMap);
            }
        } else if (selectBody instanceof SetOperationList setOperationList) {
            // If the SelectBody is a SetOperationList, get all the selects in the operation
            List<SelectBody> selectBodies = setOperationList.getSelects();
            // Iterate over each SelectBody in the operation
            for (SelectBody body : selectBodies) {
                // Recursively handle each SelectBody
                handleSelectBody(body, renameMap);
            }
        }
    }

    /**
     * Transforms the original map to a new map with values being all direct and indirect associated strings.
     * It uses a Depth-First Search (DFS) strategy to recursively find all associated strings for each key.
     *
     * @param originalMap The input map containing the relationships between strings.
     * @return The transformed map where each key is associated with all direct and indirect values from the original map.
     */
    public static Map<String, Set<String>> transform(Map<String, Set<String>> originalMap) {
        // The result map to return
        Map<String, Set<String>> resultMap = new HashMap<>();

        // Iterate through each key in the original map
        for (String key : originalMap.keySet()) {
            // If the key has not been visited, perform DFS from the key
            // The set to keep track of visited keys to avoid cycles
            Set<String> visited = new HashSet<>();
            dfs(key, originalMap, resultMap, visited);
        }

        // Return the transformed map
        return resultMap;
    }

    /**
     * Performs a Depth-First Search (DFS) from the provided key to find all associated strings.
     *
     * @param key The key to start the DFS from.
     * @param originalMap The original map containing the relationships between strings.
     * @param resultMap The result map to add the associated strings to.
     * @param visited The set to keep track of visited keys to avoid cycles.
     * @return The set of associated strings for the provided key.
     */
    public static Set<String> dfs(String key, Map<String, Set<String>> originalMap, Map<String, Set<String>> resultMap, Set<String> visited) {
        visited.add(key);
        // The set of associated strings for the provided key
        Set<String> result = resultMap.getOrDefault(key, new HashSet<>());
        // The set of direct associated strings for the provided key
        Set<String> directValues = originalMap.get(key);

        // If the key has associated strings, iterate through each associated string
        if (directValues != null) {
            // For each associated string, add it to the result set and perform DFS from it if it has not been visited
            for (String value : directValues) {
                result.add(value);
                // If the value has not been visited, perform DFS from it
                if (!visited.contains(value)) {
                    // Add the result of the DFS to the result set
                    result.addAll(dfs(value, originalMap, resultMap, visited));
                }
            }
        }
        result.remove(key);
        // Add the result set to the result map
        if (!result.isEmpty()){
            resultMap.put(key, result);
        }
        return result;
    }


    /**
     * Determines if a column name matches a rule column, taking into account any renaming rules specified in a rename map.
     *
     * @param columnName the column name to be matched
     * @param ruleColumn the rule column that the column name should be compared with
     * @param renameMap a map containing column rename rules, where the key is the original column name and the value is the renamed column set
     * @return true if the column name matches the rule column or any of its renamed versions specified in the rename map, false otherwise
     */
    public static boolean columnMatch(String columnName, String ruleColumn, Map<String, Set<String>> renameMap) {
        Locale.setDefault(Locale.ENGLISH);
        columnName = columnName.toLowerCase();
        ruleColumn = ruleColumn.toLowerCase();

        // Direct match
        if (columnName.equals(ruleColumn)) {
            return true;
        }

        // No rename map
        if (renameMap == null || renameMap.isEmpty()) {
            return false;
        }

        return renameMap.getOrDefault(columnName, Collections.emptySet()).contains(ruleColumn);
    }


    /**
     * Processes the input string by first removing the outer quotes (if present)
     * and then extracting the substring after the last occurrence of '.' (if present).
     *
     * @param str The input string to be processed.
     * @return The processed string with outer quotes removed and only the substring after the last '.' (if present).
     * If the input string is null or less than 2 characters long, the function returns the string as is.
     * If no '.' is found in the string, the function returns the string as is.
     */
    public static String processString(String str) {
        // If the string is null or less than 2 characters long, return the string as is
        if (str == null || str.length() < 2) {
            return str;
        }

        // If the string starts and ends with a quote, remove the quotes
        if (str.startsWith("\"") && str.endsWith("\"")) {
            str = str.substring(1, str.length() - 1);
        }

        // Find the last occurrence of '.'
        int lastIndex = str.lastIndexOf(".");
        // If '.' was found, return the substring after the last '.', else return the string as is
        return lastIndex == -1 ? str : str.substring(lastIndex + 1);
    }

    /**
     * Converts a byte array to a hexadecimal string.
     *
     * @param bytes The byte array to be converted.
     * @return The hexadecimal string representation of the byte array.
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * This method is used to handle binary data for both byte[] and String inputs.
     *
     * @param data The data to be converted, which can be a byte array (byte[]) or a string (String).
     * @return A String representation of the hex value of the input data.
     *
     * @throws IllegalArgumentException If the data is neither a byte array nor a string.
     */
    public static String handBinaryData(Object data) {
        byte[] bytes;
        // If the input data is a byte array, assign it directly to bytes.
        if (data instanceof byte[] dataByte) {
            bytes = dataByte;
        }
        // If the input data is a String, convert the string to a byte array using UTF-8 encoding, and then assign it to bytes.
        else if (data instanceof String string) {
            bytes = string.getBytes(StandardCharsets.UTF_8);
        }
        // If the input data is neither a byte array nor a string, throw an IllegalArgumentException.
        else {
            throw new IllegalArgumentException("BLOB type requires data to be byte[] or String");
        }
        // Convert the byte array to a hex string and return it.
        return bytesToHex(bytes);
    }

    /**
     * Formats the provided date object into a String representation.
     * This method accepts objects of types Timestamp, Time, and Date. It returns
     * the formatted string representation of the date based on its type. If the object
     * is not of the mentioned types, it returns the string representation of the object.
     *
     * @param data The date object to be formatted. It can be of type Timestamp, Time, Date, or any other object.
     * @return A formatted string representation of the provided date object. If the object is not of the expected date types, returns the string representation of the object itself.
     */
    public static String handDateData(Object data) {
        SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

        if (data instanceof Timestamp timestamp) {
            return "'" + timestampFormatter.format(timestamp) + "'";
        } else if (data instanceof Time time) {
            return "'" + timeFormatter.format(time) + "'";
        } else if (data instanceof Date date) {
            return "'" + dateFormatter.format(date) + "'";
        } else {
            return "'" + data + "'";
        }
    }

    /**
     * Splits a SQL script into a list of individual SQL statements.
     *
     * @param sqlScript The SQL script to be split into individual statements. The script can contain
     *                  any number of SQL statements, separated by semicolons. The statements can be of any
     *                  type (SELECT, INSERT, UPDATE, DELETE, etc.), and can include complex structures such
     *                  as stored procedures.
     * @return A List of String, each of which is a single SQL statement from the original script.
     * @throws JSQLParserException If the provided SQL script cannot be parsed. This could occur if the
     *                             script contains syntax errors, or uses features of SQL not supported by
     *                             the parser.
     */
    public static List<String> splitSqlScript(String sqlScript) throws JSQLParserException {
        List<net.sf.jsqlparser.statement.Statement> statements = CCJSqlParserUtil.parseStatements(sqlScript).getStatements();
        return statements.stream()
                .map(net.sf.jsqlparser.statement.Statement::toString)
                .collect(Collectors.toList());
    }
}
