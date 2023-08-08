package com.dbmasker.database;

import com.dbmasker.database.gbase.GBase8a;
import com.dbmasker.database.gbase.Gbase8s;
import com.dbmasker.database.gbase.Gbase8t;
import com.dbmasker.utils.ErrorMessages;

/**
 * DatabaseFactory class is responsible for creating instances of Database interface implementations
 * based on the provided database type.
 */
public class DatabaseFactory {

    /**
     * Default constructor for DatabaseFactory class.
     */
    public DatabaseFactory() {
        super();
    }

    /**
     * Creates and returns an instance of the appropriate Database interface implementation
     * based on the provided database type.
     *
     * @param dbType A string representing the database type (e.g., "sqlite", "mysql").
     * @return An instance of the corresponding Database interface implementation.
     * @throws UnsupportedOperationException If the provided database type is not supported.
     */
    public Database getDatabase(String dbType) {
        if (dbType == null) {
            return null;
        }

        DbType dbTypeEnum;
        try {
            dbTypeEnum = DbType.valueOf(dbType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException(ErrorMessages.UNSUPPORTED_DATABASE_TYPE_ERROR + dbType);
        }

        return switch (dbTypeEnum) {
            case SQLITE -> new SQLite();
            case MYSQL -> new MySQL();
            case MARIADB -> new MariaDB();
            case ORACLE -> new Oracle();
            case KINGBASE -> new KingBase();
            case POSTGRESQL -> new PostgreSQL();
            case MSSQL -> new MsSQL();
            case DM -> new DaMeng();
            case GBASE8A -> new GBase8a();
            case GBASE8S -> new Gbase8s();
            case GBASE8T -> new Gbase8t();
            case HIVE -> new Hive();
            case ELASTICSEARCH -> new ElasticSearch();
            case PHOENIX -> new Phoenix();
            default -> throw new UnsupportedOperationException(ErrorMessages.UNSUPPORTED_DATABASE_TYPE_ERROR + dbType);
        };
    }
}
