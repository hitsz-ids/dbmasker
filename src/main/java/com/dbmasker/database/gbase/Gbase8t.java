package com.dbmasker.database.gbase;

/**
 * Gbase8t Database class implements the Database interface for Gbase8t databases.
 * Provides an implementation to retrieve a list of schemas (databases) from a GBase8s database.
 */
public class Gbase8t extends Gbase8s {

    /**
     * Default constructor for Gbase8t class.
     */
    public Gbase8t() {
        super();
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
        return String.format("SELECT SKIP %d FIRST %d %s FROM %s", offset, pageSize, columns, tableName);
    }
}
