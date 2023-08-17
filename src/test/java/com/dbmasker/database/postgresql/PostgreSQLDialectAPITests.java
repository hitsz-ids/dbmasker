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

    public void createSchema2(Connection connection, String dbType) throws SQLException {
        String sql = """
                  CREATE SCHEMA my_schema;
                  CREATE SEQUENCE my_schema.employees_id_seq;
                  
                  CREATE TABLE my_schema.employees (
                      id INTEGER PRIMARY KEY,
                      first_name VARCHAR(255) NOT NULL,
                      last_name VARCHAR(255) NOT NULL,
                      email VARCHAR(255) NOT NULL,
                      age INTEGER,
                      UNIQUE(last_name, first_name)
                  );
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createSchema3(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE SCHEMA my_schema;
                CREATE TABLE my_schema.MyTable (
                      MyBoolean BOOLEAN,
                      MyInteger INTEGER PRIMARY KEY,
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

    public void insertData2(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.employees (id, first_name, last_name, email, age)
                VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30);
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

    @Test
    void testGenerateUpdateSql() throws SQLException {
        createSchema2(connection, dbType);
        insertData2(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("age", 100);
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("id", 1);
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "employees", setData, whereData, true);
        String expectSQL = "UPDATE my_schema.employees SET age = 100  WHERE id = 1;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(100, result.get(0).get("age"));
        Assertions.assertEquals(1, result.get(0).get("id"));

        setData.put("age", 10);
        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("email", "john.doe@example.com");
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "employees", setData, whereData, true);
        expectSQL = "UPDATE my_schema.employees SET age = 10  WHERE last_name = 'Doe' AND first_name = 'John';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(10, result.get(0).get("age"));
        Assertions.assertEquals(1, result.get(0).get("id"));

        setData.put("age", 99);
        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("email", "john.doe@example.com");
        whereData.put("age", 10);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "employees", setData, whereData, true);
        expectSQL = "UPDATE my_schema.employees SET age = 99  WHERE first_name = 'John' AND email = 'john.doe@example.com' AND age = 10;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(99, result.get(0).get("age"));
        Assertions.assertEquals(1, result.get(0).get("id"));

        setData.put("age", 98);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "employees", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(99, result.get(0).get("age"));

        whereData = null;
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "employees", setData, whereData, true);
        Assertions.assertNull(sql);
    }

    @Test
    void testGenerateUpdateSql1() throws SQLException {
        createSchema3(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("MyBoolean", false);
        setData.put("MyChar", "new char");
        setData.put("MyTimestampWithoutTimeZone", "2023-7-19 10:23:54");
        setData.put("MyBytea", "Masker".getBytes());
        setData.put("MyInt", null);
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("MyInteger", 10);
        whereData.put("MyIntervalYear", "1 year");

        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "mytable", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(String.format("%-50s", "new char"), result.get(0).get("mychar"));
        Assertions.assertEquals(false, result.get(0).get("myboolean"));
        Assertions.assertEquals("2023-07-19 10:23:54.0", result.get(0).get("mytimestampwithouttimezone").toString());
        Assertions.assertEquals("Masker", new String((byte[]) result.get(0).get("mybytea")));

        setData = new HashMap<>();
        setData.put("MyInteger", 1);
        whereData.put("MyBoolean", false);
        whereData.put("MyChar", "new char");
        whereData.put("MyTimestampWithoutTimeZone", "2023-7-19 10:23:54");
        whereData.put("MyBytea", "Masker".getBytes());
        whereData.put("MyInt", null);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "mytable", setData, whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).get("myinteger"));
    }

    @Test
    void testGenerateDeleteSql() throws SQLException, ClassNotFoundException {
        createSchema2(connection, dbType);
        insertData2(connection, dbType);

        Map<String, Object> whereData = new HashMap<>();
        whereData.put("id", 1);
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        String sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "employees", whereData, true);
        String expectSQL = "DELETE FROM my_schema.employees WHERE id = 1;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(0, result.size());

        insertData2(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("email", "john.doe@example.com");

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "employees", whereData, true);
        expectSQL = "DELETE FROM my_schema.employees WHERE last_name = 'Doe' AND first_name = 'John';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(0, result.size());

        insertData2(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("email", "john.doe@example.com");
        whereData.put("age", 30);

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "employees", whereData, true);
        expectSQL = "DELETE FROM my_schema.employees WHERE first_name = 'John' AND email = 'john.doe@example.com' AND age = 30;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(0, result.size());

        insertData2(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());

        whereData.put("age", 31);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "employees", whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = null;
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "employees", whereData, true);
        Assertions.assertNull(sql);

        tearDown();
        setUp();
        createSchema3(connection, dbType);
        insertData(connection, dbType);

        whereData = new HashMap<>();
        whereData.put("MyBoolean", true);
        whereData.put("MyChar", "char50");
        whereData.put("MyTimestampWithoutTimeZone", "2004-10-19 10:23:54");
        byte[] byteArray = new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        whereData.put("MyBytea", byteArray);
        whereData.put("MyInt", 20);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "mytable", whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(0, result.size());
    }
}
