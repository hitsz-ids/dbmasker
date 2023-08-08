package com.dbmasker.dialect;

import com.dbmasker.database.*;
import com.dbmasker.dialect.gbase.Gbase8aDialect;
import com.dbmasker.dialect.gbase.Gbase8sDialect;
import com.dbmasker.dialect.gbase.Gbase8tDialect;
import com.dbmasker.utils.ErrorMessages;

/**
 * DialectFactory class is responsible for creating instances of Dialect interface implementations
 * based on the provided dialect type.
 */
public class DialectFactory {

    /**
     * Default constructor for DialectFactory class.
     */
    public DialectFactory() {
        super();
    }

    /**
     * Creates and returns an instance of the appropriate Dialect interface implementation
     * based on the provided database type.
     *
     * @param dbType A string representing the database type (e.g., "sqlite", "mysql").
     * @return An instance of the corresponding Dialect interface implementation.
     * @throws UnsupportedOperationException If the provided database type is not supported.
     */
    public Dialect getDialect(String dbType) {
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
            case SQLITE -> new SQLiteDialect();
            case POSTGRESQL -> new PostgreSQLDialect();
            case MSSQL -> new MsSQLDialect();
            case ORACLE -> new OracleDialect();
            case MYSQL -> new MySQLDialect();
            case MARIADB -> new MariaDBDialect();
            case DM -> new DMDialect();
            case KINGBASE -> new KingBaseDialect();
            case GBASE8A -> new Gbase8aDialect();
            case GBASE8S -> new Gbase8sDialect();
            case GBASE8T -> new Gbase8tDialect();
            case HIVE -> new HiveDialect();
            case PHOENIX -> new PhoenixDialect();
            default -> throw new UnsupportedOperationException(ErrorMessages.UNSUPPORTED_DATABASE_TYPE_ERROR + dbType);
        };
    }
}
