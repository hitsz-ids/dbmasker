package com.dbmasker.database;

/**
 * DbType is an enumeration representing various database types.
 * <p>
 * It includes a predefined set of database types, such as SQLITE, MYSQL, DM, KINGBASE, and GBASE.
 * Each enumeration item contains a string representation of the database name, which can be
 * accessed using the {@link #getDbName()} method.
 */
public enum DbType {
    /**
     * The SQLITE database type.
     */
    SQLITE("sqlite"),
    /**
     * The MYSQL database type.
     */
    MYSQL("mysql"),
    /**
     * The MARIADB database type.
     */
    MARIADB("mariadb"),
    /**
     * The ORACLE database type.
     */
    ORACLE("oracle"),
    /**
     * The POSTGRESQL database type.
     */
    POSTGRESQL("postgresql"),
    /**
     * The MSSQL database type.
     */
    MSSQL("mssql"),
    /**
     * The DM database type.
     */
    DM("dm"),
    /**
     * The KINGBASE database type.
     */
    KINGBASE("kingbase"),
    /**
     * The GBASE8A database type.
     */
    GBASE8A("gbase8a"),
    /**
     * The GBASE8S database type.
     */
    GBASE8S("gbase8s"),
    /**
     * The GBASE8T database type.
     */
    GBASE8T("gbase8t"),

    /**
     * The HIVE database type.
     */
    HIVE("hive"),
    /**
     * The ELASTICSEARCH database type.
     */
    ELASTICSEARCH("elasticsearch"),
    /**
     * The Hbase Phoenix database type.
     */
    PHOENIX("phoenix");

    // The name of the database type
    private final String dbName;

    /**
     * Constructs a DbType enumeration item with the given database name.
     *
     * @param dbName the name of the database type
     */
    DbType(String dbName) {
        this.dbName = dbName;
    }

    /**
     * Returns the name of the database type.
     *
     * @return the database name
     */
    public String getDbName() {
        return dbName;
    }
}
