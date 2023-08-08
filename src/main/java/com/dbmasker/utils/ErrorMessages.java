package com.dbmasker.utils;

/**
 * The ErrorMessages class contains constants for various error messages.
 * This class is final and cannot be instantiated.
 */
public final class ErrorMessages {

    /**
     * Private constructor to prevent instantiation.
     * An exception is thrown if an attempt is made.
     */
    private ErrorMessages() {
        throw new UnsupportedOperationException();
    }

    /**
     * Error message when connection or database type is null
     */
    public static final String NULL_CONNECTION_OR_DB_TYPE_ERROR = "Connection or database type must not be null.";

    /**
     * Error message when table or view name is null.
     */
    public static final String NULL_TABLE_OR_VIEW_NAME_ERROR = "Table or view name must not be null.";

    /**
     * Error message when table not found.
     */
    public static final String TABLE_NOT_FOUND_ERROR = "Table not found";

    /**
     * Error message when sql is null.
     */
    public static final String NULL_SQL_ERROR = "Sql must not be null.";

    /**
     * Error message when obfuscation rules is null.
     */
    public static final String NULL_OBFUSCATION_RULES_ERROR = "obfuscation rules must not be null.";

    /**
     * Error message when sql list is null.
     */
    public static final String NULL_SQL_LIST_ERROR = "Sql list must not be null.";

    /**
     * Error message when sql script is null.
     */
    public static final String NULL_SQL_SCRIPT_ERROR = "Sql script must not be null.";

    /**
     * Error message when regex list is null.
     */
    public static final String NULL_REGEX_LIST_ERROR = "regex list must not be null.";

    /**
     * Error message when a connection cannot be closed.
     */
    public static final String FAILED_TO_CLOSE_CONNECTION_ERROR = "Failed to close the connection: ";

    /**
     * Error message when the JDBC driver is not found.
     */
    public static final String JDBC_DRIVER_NOT_FOUND_ERROR = "JDBC driver not found: ";

    /**
     * Error message when a connection cannot be established.
     */
    public static final String CONNECTION_ESTABLISHMENT_FAILURE_ERROR = "Failed to establish a connection: ";

    /**
     * Error message when a transaction cannot be rolled back.
     */
    public static final String TRANSACTION_ROLLBACK_ERROR = "Error rolling back transaction: ";

    /**
     * Error message when batch update SQL statements cannot be executed.
     */
    public static final String BATCH_UPDATE_SQL_EXECUTION_ERROR = "Error executing batch update SQL statements: ";

    /**
     * Error message when a function cannot be executed.
     */
    public static final String FUNCTION_EXECUTION_ERROR = "Error executing function ";

    /**
     * Error message when a table cannot be scanned.
     */
    public static final String TABLE_SCANNING_ERROR = "Error scanning table ";

    /**
     * Error message when schema names cannot be retrieved.
     */
    public static final String SCHEMA_NAMES_RETRIEVAL_ERROR = "Unable to retrieve schema names";

    /**
     * Error message when table names cannot be retrieved.
     */
    public static final String TABLE_NAMES_RETRIEVAL_ERROR = "Unable to retrieve table names";

    /**
     * Error message when function names cannot be retrieved.
     */
    public static final String FUNCTION_NAMES_RETRIEVAL_ERROR = "Unable to retrieve function names";

    /**
     * Error message when an unsupported database type is encountered.
     */
    public static final String UNSUPPORTED_DATABASE_TYPE_ERROR = "Unsupported database type: ";
}
