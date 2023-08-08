package com.dbmasker.database.postgresql;

import com.dbmasker.api.DBDialectManager;
import com.dbmasker.api.DBManager;
import com.dbmasker.database.DbType;
import com.dbmasker.dialect.Dialect;
import com.dbmasker.dialect.DialectFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

class PostgreSQLDialectAPITests {
    private String driver = "org.postgresql.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.POSTGRESQL.getDbName();
    private String version = "v9";

    private Connection connection;

    public void createSchema(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE SCHEMA my_schema;
                CREATE TABLE my_schema.MyTable (
                      MyBoolean BOOLEAN,
                      MyInteger INTEGER,
                      MyInt INT,
                      MyBigInt BIGINT,
                      MySmallInt SMALLINT,
                      MyChar CHAR(50),
                      MyVarchar VARCHAR(50),
                      MyText TEXT,
                      MyReal REAL,
                      MyBytea BYTEA,
                      MyDoublePrecision DOUBLE PRECISION,
                      MyDecimal DECIMAL,
                      MyNumeric NUMERIC,
                      MyTimeWithoutTimeZone TIME WITHOUT TIME ZONE,
                      MyTimeWithTimeZone TIME WITH TIME ZONE,
                      MyTimestampWithoutTimeZone TIMESTAMP WITHOUT TIME ZONE,
                      MyTimestampWithTimeZone TIMESTAMP WITH TIME ZONE,
                      MySerial SERIAL,
                      MySmallSerial SMALLSERIAL,
                      MyBigSerial BIGSERIAL,
                      MyInterval INTERVAL,
                      MyIntervalYear INTERVAL YEAR
                  );
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.MyTable (
                    MyBoolean,
                    MyInteger,
                    MyInt,
                    MyBigInt,
                    MySmallInt,
                    MyChar,
                    MyVarchar,
                    MyText,
                    MyReal,
                    MyBytea,
                    MyDoublePrecision,
                    MyDecimal,
                    MyNumeric,
                    MyTimeWithoutTimeZone,
                    MyTimeWithTimeZone,
                    MyTimestampWithoutTimeZone,
                    MyTimestampWithTimeZone,
                    MySerial,
                    MySmallSerial,
                    MyBigSerial,
                    MyInterval,
                    MyIntervalYear
                ) VALUES (
                    TRUE, -- MyBoolean
                    10, -- MyInteger
                    20, -- MyInt
                    1000000000000, -- MyBigInt
                    5, -- MySmallInt
                    'char50', -- MyChar
                    'varchar50', -- MyVarchar
                    'text', -- MyText
                    1.234, -- MyReal
                    E'\\\\xDEADBEEF', -- MyBytea
                    1.23456789012345, -- MyDoublePrecision
                    10.5, -- MyDecimal
                    123456789.12345, -- MyNumeric
                    '01:02:03', -- MyTimeWithoutTimeZone
                    '01:02:03-04', -- MyTimeWithTimeZone
                    '2004-10-19 10:23:54', -- MyTimestampWithoutTimeZone
                    '2004-10-19 10:23:54+02', -- MyTimestampWithTimeZone
                    DEFAULT, -- MySerial
                    DEFAULT, -- MySmallSerial
                    DEFAULT, -- MyBigSerial
                    '1 year', -- MyInterval
                    '1 year' -- MyIntervalYear
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/postgreSQL.properties")) {
            properties.load(in);
            this.url = properties.getProperty("url");
            this.username = properties.getProperty("username");
            this.password = properties.getProperty("password");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    public void setUp() throws SQLException, ClassNotFoundException {
        initConfig();

        connection = DBManager.createConnection(driver, url, username, password);
    }

    @AfterEach
    public void tearDown() throws SQLException {
        String sql1 = "DROP SCHEMA IF EXISTS my_schema CASCADE;";
        DBManager.executeUpdateSQL(connection, dbType, sql1);
        DBManager.closeConnection(connection);
    }


    @Test
    void testFormatData() {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "INTEGER"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "REAL"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "TEXT"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Assertions.assertEquals("true", dialect.formatData(true, "BOOLEAN"));
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData("2004-10-19 10:23:54", "TIMESTAMPTZ"));
        // Test BLOB type
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        Assertions.assertEquals("E'\\\\x01020304'", dialect.formatData(data, "BYTEA"));

        String str = "DBMasker";
        Assertions.assertEquals("E'\\\\x44424d61736b6572'", dialect.formatData(str, "BYTEA"));
    }

    @Test
    void testGenerateInsertSql() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> data = new HashMap<>();
        data.put("MyBoolean", true);
        data.put("MyInteger", 10);
        data.put("MyInt", 20);
        data.put("MyBigInt", new BigInteger("1000000000000"));
        data.put("MySmallInt", 5);
        data.put("MyChar", "char50");
        data.put("MyVarchar", "varchar50");
        data.put("MyText", "text");
        data.put("MyReal", 1.234);
        data.put("MyBytea", "DBMasker");
        data.put("MyDoublePrecision", 1.23456789012345);
        data.put("MyDecimal", 10.5);
        data.put("MyNumeric", new BigDecimal("123456789.12345"));
        data.put("MyTimeWithoutTimeZone", "01:02:03");
        data.put("MyTimeWithTimeZone", "01:02:03-04");
        data.put("MyTimestampWithoutTimeZone", "2004-10-19 10:23:54");
        data.put("MyTimestampWithTimeZone", "2004-10-19 10:23:54+02");
        data.put("MySerial", "DEFAULT");
        data.put("MySmallSerial", "DEFAULT");
        data.put("MyBigSerial", "DEFAULT");
        data.put("MyInterval", "1 year");
        data.put("MyIntervalYear", "1 year");

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(2, result.size());

        Assertions.assertEquals(true, result.get(1).get("myboolean"));
        Assertions.assertEquals(Long.valueOf("1000000000000"), result.get(1).get("mybigint"));
        Assertions.assertEquals(1.23456789012345, result.get(1).get("mydoubleprecision"));
        Assertions.assertEquals(2, result.get(1).get("myserial"));
        Assertions.assertArrayEquals("DBMasker".getBytes(), (byte[])result.get(1).get("mybytea"));

        data.put("MyChar", "char'50'");
        data.put("MyTimestampWithTimeZone", "''2004-10-19 10:23:54+02");
        data.put("MyBytea", "DBMasker".getBytes());
        data.put("MyBoolean", "False");
        sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(String.format("%-50s", "char'50'"), result.get(2).get("mychar"));
        Assertions.assertEquals("2004-10-19 16:23:54.0", result.get(2).get("mytimestampwithtimezone").toString());
        Assertions.assertEquals(false, result.get(2).get("myboolean"));

        data.put("MyInteger", null);
        data.put("MyIntervalYear", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("myinteger"));
        Assertions.assertNull(result.get(3).get("myintervalyear"));
    }
}
