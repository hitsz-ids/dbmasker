package com.dbmasker.database.mssql;

import com.dbmasker.api.DBDialectManager;
import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.database.DbType;
import com.dbmasker.dialect.Dialect;
import com.dbmasker.dialect.DialectFactory;
import com.dbmasker.utils.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

class MsSQLDialectAPITests {
    private String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.MSSQL.getDbName();
    private String version = "v13";

    private Connection connection;

    public void createSchema(Connection connection, String dbType) throws SQLException {
        String sql1 = "CREATE SCHEMA my_schema;";
        DBManager.executeUpdateSQL(connection, dbType, sql1);
        String sql = """
                CREATE TABLE my_schema.MyTable (
                    MyBigInt BIGINT,
                    MyInt INT,
                    MySmallInt SMALLINT,
                    MyMoney MONEY,
                    MySmallMoney SMALLMONEY,
                    MyNumeric NUMERIC(18, 0),
                    MyDecimal DECIMAL(18, 0),
                    MyBit BIT,
                    MyTinyInt TINYINT,
                    MyFloat FLOAT,
                    MyReal REAL,
                    MyDate DATE,
                    MyDatetime DATETIME,
                    MyDatetime2 DATETIME2,
                    MySmallDatetime SMALLDATETIME,
                    MyTime TIME,
                    MyDatetimeoffset DATETIMEOFFSET,
                    MyChar CHAR(50),
                    MySqlVariant SQL_VARIANT,
                    MyVarchar VARCHAR(50),
                    MyText TEXT,
                    MyNChar NCHAR(50),
                    MyNVarChar NVARCHAR(50),
                    MyNText NTEXT,
                    MyBinary BINARY(50),
                    MyVarBinary VARBINARY(50),
                    MyImage IMAGE,
                    MyXml XML,
                    MyGeometry GEOMETRY,
                    MyGeography GEOGRAPHY
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createTable1(Connection connection, String dbType) throws SQLException {
        String sql = """
                  CREATE TABLE my_schema.employees (
                     id INT PRIMARY KEY,
                     first_name NVARCHAR(255) NOT NULL,
                     last_name NVARCHAR(255) NOT NULL,
                     email NVARCHAR(255) NOT NULL,
                     age INT,
                     UNIQUE (first_name, last_name)
                  );
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.employees (id, first_name, last_name, email, age)
                VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30);
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.MyTable (
                   MyBigInt,
                   MyInt,
                   MySmallInt,
                   MyMoney,
                   MySmallMoney,
                   MyNumeric,
                   MyDecimal,
                   MyBit,
                   MyTinyInt,
                   MyFloat,
                   MyReal,
                   MyDate,
                   MyDatetime,
                   MyDatetime2,
                   MySmallDatetime,
                   MyTime,
                   MyDatetimeoffset,
                   MyChar,
                   MySqlVariant,
                   MyVarchar,
                   MyText,
                   MyNChar,
                   MyNVarChar,
                   MyNText,
                   MyBinary,
                   MyVarBinary,
                   MyImage,
                   MyXml,
                   MyGeometry,
                   MyGeography
                )
                VALUES (
                   123456789,  -- bigint
                   12345,  -- int
                   123,  -- smallint
                   123.45,  -- money
                   123.45,  -- smallmoney
                   123,  -- numeric
                   123,  -- decimal
                   1,  -- bit
                   123,  -- tinyint
                   123.45,  -- float
                   123.45,  -- real
                   '2023-01-01',  -- date
                   '2023-01-01 00:00:00',  -- datetime
                   '2023-01-01 00:00:00',  -- datetime2
                   '2023-01-01 00:00:00',  -- smalldatetime
                   '00:00:00',  -- time
                   '2023-01-01 00:00:00 +00:00',  -- datetimeoffset
                   'abc',  -- char
                   'abc',  -- sql_variant
                   'abc',  -- varchar
                   'abc',  -- text
                   N'abc',  -- nchar
                   N'abc',  -- nvarchar
                   N'abc',  -- ntext
                   0x0123456789ABCDEF,  -- binary
                   0x0123456789ABCDEF,  -- varbinary
                   0x0123456789ABCDEF,  -- image
                   '<root></root>',  -- xml
                   NULL,  -- geometry
                   NULL  -- geography
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/mssql.properties")) {
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

        createSchema(connection, dbType);
    }

    @AfterEach
    public void tearDown() throws SQLException {
        String sql = """
                IF OBJECT_ID('my_schema.MyTable', 'U') IS NOT NULL
                   DROP TABLE my_schema.MyTable;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        sql = """
                IF OBJECT_ID('my_schema.employees', 'U') IS NOT NULL
                   DROP TABLE my_schema.employees;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        String sql1 = "DROP SCHEMA my_schema;";
        DBManager.executeUpdateSQL(connection, dbType, sql1);

        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testFormatData() {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "BIGINT"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "REAL"));
        Assertions.assertEquals("N'Hello, World!'", dialect.formatData("Hello, World!", "TEXT"));
        Assertions.assertEquals("'<root></root>'", dialect.formatData("<root></root>", "XML"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Date date = new java.util.Date();
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        Assertions.assertEquals("'"+ sdf.format(date) + "'", dialect.formatData(date, "DATETIME"));
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        Assertions.assertEquals("0x01020304", dialect.formatData(data, "VARBINARY"));

        String str = "DBMasker";
        Assertions.assertEquals("0x44424d61736b6572", dialect.formatData(str, "BINARY"));
    }


    @Test
    void testGenerateInsertSql() throws SQLException {
        insertData(connection, dbType);

        Map<String, Object> data = new HashMap<>();
        data.put("MyBigInt", 123456789L);
        data.put("MyInt", 12345);
        data.put("MySmallInt", 123);
        data.put("MyMoney", 123.45);
        data.put("MySmallMoney", 123.45);
        data.put("MyNumeric", 123);
        data.put("MyDecimal", 123);
        data.put("MyBit", 1);
        data.put("MyTinyInt", 123);
        data.put("MyFloat", 123.45);
        data.put("MyReal", 123.45);
        data.put("MyDate", "2023-01-01");
        data.put("MyDatetime", "2023-01-01 00:00:00");
        data.put("MyDatetime2", "2023-01-01 00:00:00");
        data.put("MySmallDatetime", "2023-01-01 00:00:00");
        data.put("MyTime", "00:00:00");
        data.put("MyDatetimeoffset", "2023-01-01 00:00:00 +00:00");
        data.put("MyChar", "abc");
        data.put("MySqlVariant", "abc");
        data.put("MyVarchar", "abc");
        data.put("MyText", "abc");
        data.put("MyNChar", "abc");
        data.put("MyNVarChar", "abc");
        data.put("MyNText", "abc");
        data.put("MyBinary", "DBMasker");
        data.put("MyVarBinary", "DBMasker");
        data.put("MyImage", "DBMasker");
        data.put("MyXml", "<root></root>");
        data.put("MyGeometry", null);
        data.put("MyGeography", null);

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(2, result.size());

        Assertions.assertEquals(true, result.get(1).get("MyBit"));
        Assertions.assertEquals(123456789L, result.get(1).get("MyBigInt"));
        Assertions.assertEquals("2023-01-01 00:00:00 +00:00", result.get(1).get("MyDatetimeoffset").toString());
        Assertions.assertArrayEquals("DBMasker".getBytes(), (byte[])result.get(1).get("MyVarBinary"));

        data.put("MyChar", "char'50'");
        data.put("MyDatetimeoffset", "2004-10-19 10:23:54 +02:00");
        data.put("MyVarBinary", "DBMasker".getBytes());
        data.put("MyBit", 0);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(String.format("%-50s", "char'50'"), result.get(2).get("MyChar"));
        Assertions.assertEquals("2004-10-19 10:23:54 +02:00", result.get(2).get("MyDatetimeoffset").toString());
        Assertions.assertEquals(false, result.get(2).get("MyBit"));
        Assertions.assertArrayEquals("DBMasker".getBytes(), (byte[])result.get(1).get("MyVarBinary"));

        data.put("MyXml", null);
        data.put("MyNText", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("MyXml"));
        Assertions.assertNull(result.get(3).get("MyNText"));
    }

    @Test
    void testGenerateUpdateSql() throws SQLException {
        createTable1(connection, dbType);
        insertData1(connection, dbType);

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
        expectSQL = "UPDATE my_schema.employees SET age = 10  WHERE last_name = N'Doe' AND first_name = N'John';";
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
        expectSQL = "UPDATE my_schema.employees SET age = 99  WHERE first_name = N'John' AND email = N'john.doe@example.com' AND age = 10;";
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
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("MyChar", "char'50'");
        setData.put("MyDatetimeoffset", "2024-10-19 10:23:54 +02:00");
        setData.put("MyVarBinary", "Masker".getBytes());
        setData.put("MyBit", 0);
        setData.put("MyMoney", null);
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("MyInt", 12345);

        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "mytable", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(String.format("%-50s", "char'50'"), result.get(0).get("MyChar"));
        Assertions.assertEquals("2024-10-19 10:23:54 +02:00", result.get(0).get("MyDatetimeoffset").toString());
        Assertions.assertEquals(false, result.get(0).get("MyBit"));
        Assertions.assertArrayEquals("Masker".getBytes(), (byte[])result.get(0).get("MyVarBinary"));
        Assertions.assertNull(result.get(0).get("MyMoney"));

        setData = new HashMap<>();
        setData.put("MyInt", 999);
        whereData = new HashMap<>();
        whereData.put("MyInt", 12345);
        whereData.put("MyChar", "char'50'");
        whereData.put("MyDatetimeoffset", "2024-10-19 10:23:54 +02:00");
        whereData.put("MyVarBinary", "Masker".getBytes());
        whereData.put("MyBit", 0);
        whereData.put("MyMoney", null);

        sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "mytable", setData, whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(999, result.get(0).get("MyInt"));
    }

    @Test
    void testGenerateDeleteSql() throws SQLException, ClassNotFoundException {
        createTable1(connection, dbType);
        insertData1(connection, dbType);

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

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("email", "john.doe@example.com");

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "employees", whereData, true);
        expectSQL = "DELETE FROM my_schema.employees WHERE last_name = N'Doe' AND first_name = N'John';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("email", "john.doe@example.com");
        whereData.put("age", 30);

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "employees", whereData, true);
        expectSQL = "DELETE FROM my_schema.employees WHERE first_name = N'John' AND email = N'john.doe@example.com' AND age = 30;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
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

        insertData(connection, dbType);

        whereData = new HashMap<>();
        whereData.put("MyInt", 12345);
        whereData.put("MyChar", "abc");
        whereData.put("MyDatetimeoffset", "2023-01-01 00:00:00 +00:00");
        byte[] byteArray = new byte[] {(byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        whereData.put("MyVarBinary", byteArray);
        whereData.put("MyBit", 1);
        whereData.put("MyMoney", 123.45);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "mytable", whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(0, result.size());
    }

}
