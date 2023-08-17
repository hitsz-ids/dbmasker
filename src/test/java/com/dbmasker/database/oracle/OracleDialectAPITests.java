package com.dbmasker.database.oracle;

import com.dbmasker.api.DBDialectManager;
import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.data.SensitiveColumn;
import com.dbmasker.database.DbType;
import com.dbmasker.dialect.Dialect;
import com.dbmasker.dialect.DialectFactory;
import com.dbmasker.utils.Config;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import com.dbmasker.utils.ObfuscationMethod;
import oracle.sql.INTERVALDS;
import oracle.sql.TIMESTAMP;
import oracle.xdb.XMLType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.util.*;

class OracleDialectAPITests {

    private String driver = "oracle.jdbc.driver.OracleDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.ORACLE.getDbName();
    private String version = "v11";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE MyTable (
                    MyNumber NUMBER,
                    MyFloat FLOAT,
                    MyInteger INTEGER,
                    MySmallint SMALLINT,
                    MyReal REAL,
                    MyDoublePrecision DOUBLE PRECISION,
                    MyDecimal DECIMAL(8, 2),
                    MyChar CHAR(50),
                    MyVarchar2 VARCHAR2(50),
                    MyLong LONG,
                    MyClob CLOB,
                    MyRaw RAW(2000),
                    MyBlob BLOB,
                    MyBFile BFILE,
                    MyRowid ROWID,
                    MyUrowid VARCHAR2(200),
                    MyXml XMLType,
                    MyDate DATE,
                    MyTimestamp TIMESTAMP,
                    MyIntervalYearToMonth INTERVAL YEAR TO MONTH,
                    MyIntervalDayToSecond INTERVAL DAY TO SECOND
                )
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createTable1(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE MyTable (
                    MyNumber NUMBER,
                    MyFloat FLOAT,
                    MyInteger INTEGER,
                    MySmallint SMALLINT,
                    MyReal REAL,
                    MyDoublePrecision DOUBLE PRECISION,
                    MyDecimal DECIMAL(8, 2),
                    MyChar CHAR(50),
                    MyVarchar2 VARCHAR2(50),
                    MyLong LONG,
                    MyClob CLOB,
                    MyRaw RAW(2000),
                    MyBlob BLOB,
                    MyBFile BFILE,
                    MyRowid ROWID,
                    MyUrowid VARCHAR2(200),
                    MyXml XMLType,
                    MyDate DATE,
                    MyTimestamp TIMESTAMP,
                    MyIntervalYearToMonth INTERVAL YEAR TO MONTH,
                    MyIntervalDayToSecond INTERVAL DAY TO SECOND,
                    PRIMARY KEY (MyInteger)
                )
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createTable2(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE EMPLOYEES (
                   id INT NOT NULL,
                   first_name VARCHAR2(255) NOT NULL,
                   last_name VARCHAR2(255) NOT NULL,
                   email VARCHAR2(255) NOT NULL,
                   age INT,
                   PRIMARY KEY (id),
                   UNIQUE (first_name, last_name)
                )
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO MyTable (
                    MyNumber, MyFloat, MyInteger, MySmallint, MyReal, MyDoublePrecision,
                    MyDecimal, MyChar, MyVarchar2, MyLong, MyClob, MyRaw,
                    MyBlob, MyBFile, MyRowid, MyXml, MyDate, MyTimestamp,
                    MyIntervalYearToMonth, MyIntervalDayToSecond
                )
                VALUES (
                    123,  -- MyNumber
                    123.45,  -- MyFloat
                    123,  -- MyInteger
                    123,  -- MySmallint
                    123.45,  -- MyReal
                    123.45,  -- MyDoublePrecision
                    123.45,  -- MyDecimal
                    'CHAR',  -- MyChar
                    'VARCHAR2',  -- MyVarchar2
                    'LONG',  -- MyLong
                    TO_CLOB('This is a small CLOB'),  -- MyClob
                    HEXTORAW('DEADBEEF'),  -- MyRaw
                    HEXTORAW('44424d61736b6572'),  -- MyBlob
                    BFILENAME('DIRECTORY', 'BFILE'),  -- MyBFile
                    '00000DD5.0000.0101',  -- MyRowid
                    XMLTYPE('<root><node>XMLType</node></root>'),  -- MyXml
                    TO_DATE('2023-07-07', 'YYYY-MM-DD'),  -- MyDate
                    TIMESTAMP '2023-07-07 00:00:00',  -- MyTimestamp
                    INTERVAL '2-5' YEAR TO MONTH,  -- MyIntervalYearToMonth
                    INTERVAL '2 12:30:15' DAY TO SECOND  -- MyIntervalDayToSecond
                )
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData2(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO EMPLOYEES (id, first_name, last_name, email, age)
                VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30)
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/oracle.properties")) {
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
        String sql = """
                BEGIN
                   EXECUTE IMMEDIATE 'DROP TABLE MyTable';
                EXCEPTION
                   WHEN OTHERS THEN
                      IF SQLCODE != -942 THEN
                         RAISE;
                      END IF;
                END;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);

        sql = """
                BEGIN
                   EXECUTE IMMEDIATE 'DROP TABLE EMPLOYEES';
                EXCEPTION
                   WHEN OTHERS THEN
                      IF SQLCODE != -942 THEN
                         RAISE;
                      END IF;
                END;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);

        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testFormatData() {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "INTEGER"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "REAL"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "VARCHAR2(50)"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Assertions.assertEquals("true", dialect.formatData(true, "BOOLEAN"));

        Assertions.assertEquals("TIMESTAMP '2004-10-19 10:23:54'", dialect.formatData("2004-10-19 10:23:54", "TIMESTAMP(6)"));
        Assertions.assertEquals("TO_DATE('2004-10-19', 'yyyy-mm-dd')", dialect.formatData("2004-10-19", "DATE"));
        Assertions.assertEquals("INTERVAL '2-5' YEAR TO MONTH", dialect.formatData("2-5", "INTERVAL YEAR(2) TO MONTH"));
        Assertions.assertEquals("INTERVAL '2 12:30:15' DAY TO SECOND", dialect.formatData("2 12:30:15", "INTERVAL DAY(2) TO SECOND(6)"));

        Assertions.assertEquals("XMLTYPE('<root><node>XMLType</node></root>')", dialect.formatData("<root><node>XMLType</node></root>", "SYS.XMLTYPE(2000)"));
        // Test BLOB type
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        Assertions.assertEquals("HEXTORAW('01020304')", dialect.formatData(data, "BLOB"));
        Assertions.assertEquals("HEXTORAW('01020304')", dialect.formatData(data, "RAW(2000)"));

        String str = "DBMasker";
        Assertions.assertEquals("HEXTORAW('44424d61736b6572')", dialect.formatData(str, "BLOB"));
    }

    @Test
    void testGenerateInsertSql() throws SQLException, IOException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> data = new HashMap<>();
        data.put("MyNumber", 123);
        data.put("MyFloat", 123.45);
        data.put("MyInteger", 123);
        data.put("MySmallint", 123);
        data.put("MyReal", 123.45);
        data.put("MyDoublePrecision", 123.45);
        data.put("MyDecimal", 123.45);
        data.put("MyChar", "CHAR");
        data.put("MyVarchar2", "VARCHAR2");
        data.put("MyLong", "LONG");
        data.put("MyClob", "This is a small CLOB");
        data.put("MyRaw", "DBMasker");
        data.put("MyBlob", "DBMasker");
        data.put("MyBFile", null);
        data.put("MyRowid", "00000DD5.0000.0101");
        data.put("MyXml", "<root><node>XMLType</node></root>");
        data.put("MyDate", "2023-07-07");
        data.put("MyTimestamp", "2023-07-07 00:00:00");
        data.put("MyIntervalYearToMonth", "2-5");
        data.put("MyIntervalDayToSecond", "2 12:30:15");

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "M", "MYTABLE", data);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "M", "MYTABLE");
        Assertions.assertEquals(2, result.size());

        Assertions.assertEquals(BigDecimal.valueOf(123), result.get(1).get("MYNUMBER"));
        Assertions.assertEquals(BigDecimal.valueOf(123.45), result.get(1).get("MYFLOAT"));
        Assertions.assertEquals(BigDecimal.valueOf(123), result.get(1).get("MYINTEGER"));
        Assertions.assertEquals(BigDecimal.valueOf(123), result.get(1).get("MYSMALLINT"));
        Assertions.assertEquals(BigDecimal.valueOf(123.45), result.get(1).get("MYREAL"));
        Assertions.assertEquals(BigDecimal.valueOf(123.45), result.get(1).get("MYDOUBLEPRECISION"));
        Assertions.assertEquals(BigDecimal.valueOf(123.45), result.get(1).get("MYDECIMAL"));
        Assertions.assertEquals(String.format("%-50s", "CHAR"), result.get(1).get("MYCHAR"));
        Assertions.assertEquals("VARCHAR2", result.get(1).get("MYVARCHAR2"));
        Assertions.assertEquals("LONG", result.get(1).get("MYLONG"));

        Assertions.assertArrayEquals("DBMasker".getBytes(), (byte[])result.get(1).get("MYRAW"));
        Assertions.assertNull(result.get(1).get("MYBFILE"));
//        Assertions.assertEquals("00000DD5.0000.0101", result.get(1).get("MYROWID"));
        Assertions.assertEquals("<root><node>XMLType</node></root>", ((XMLType) result.get(1).get("MYXML")).getString());
        Assertions.assertEquals("2023-07-07 00:00:00.0", result.get(1).get("MYDATE").toString());
        Assertions.assertEquals("2023-07-07 00:00:00.0", result.get(1).get("MYTIMESTAMP").toString());
        Assertions.assertEquals("2-5", result.get(1).get("MYINTERVALYEARTOMONTH").toString());
        Assertions.assertEquals("2 12:30:15.0", result.get(1).get("MYINTERVALDAYTOSECOND").toString());

        Blob blob = (Blob)result.get(1).get("MYBLOB");
        InputStream is = blob.getBinaryStream();
        byte[] bytes = is.readAllBytes();
        Assertions.assertArrayEquals("DBMasker".getBytes(), bytes);

        Clob clob = (Clob)result.get(1).get("MYCLOB");
        Reader reader = clob.getCharacterStream();
        BufferedReader br = new BufferedReader(reader);
        Assertions.assertEquals("This is a small CLOB", br.readLine());

        data.put("MyChar", "char'50'");
        data.put("MyBlob", "DBMasker".getBytes());
        sql = DBDialectManager.generateInsertSql(connection, dbType, "M", "MYTABLE", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "M", "MYTABLE");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(String.format("%-50s", "char'50'"), result.get(2).get("MYCHAR"));

        blob = (Blob)result.get(1).get("MYBLOB");
        is = blob.getBinaryStream();
        bytes = is.readAllBytes();
        Assertions.assertArrayEquals("DBMasker".getBytes(), bytes);

        data.put("MyInteger", null);
        data.put("MyIntervalYearToMonth", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "M", "MYTABLE", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "M", "MYTABLE");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("MYINTEGER"));
        Assertions.assertNull(result.get(3).get("MYINTERVALYEARTOMONTH"));
    }

    @Test
    void testGenerateUpdateSql() throws SQLException {
        createTable2(connection, dbType);
        insertData2(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("age", 100);
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("id", 1);
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "M", "EMPLOYEES", setData, whereData, true);
        String expectSQL = "UPDATE M.EMPLOYEES SET age = 100  WHERE id = 1;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(new BigDecimal("100"), result.get(0).get("AGE"));
        Assertions.assertEquals(new BigDecimal(1), result.get(0).get("ID"));

        setData.put("age", 10);
        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("email", "john.doe@example.com");
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "M", "EMPLOYEES", setData, whereData, true);
        expectSQL = "UPDATE M.EMPLOYEES SET age = 10  WHERE last_name = 'Doe' AND first_name = 'John';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(new BigDecimal(10), result.get(0).get("AGE"));
        Assertions.assertEquals(new BigDecimal(1), result.get(0).get("ID"));

        setData.put("age", 99);
        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("email", "john.doe@example.com");
        whereData.put("age", 10);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "M", "EMPLOYEES", setData, whereData, true);
        expectSQL = "UPDATE M.EMPLOYEES SET age = 99  WHERE first_name = 'John' AND email = 'john.doe@example.com' AND age = 10;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(new BigDecimal(99), result.get(0).get("AGE"));
        Assertions.assertEquals(new BigDecimal(1), result.get(0).get("ID"));

        setData.put("age", 98);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "M", "EMPLOYEES", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(new BigDecimal(99), result.get(0).get("AGE"));

        whereData = null;
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "M", "EMPLOYEES", setData, whereData, true);
        Assertions.assertNull(sql);
    }

    @Test
    void testGenerateUpdateSql1() throws SQLException, IOException {
        createTable1(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("MyDecimal", 321.32);
        setData.put("MyChar", "char'50'");
        setData.put("MyBlob", "Masker".getBytes());
        setData.put("MyClob", "This is a big CLOB");
        setData.put("MyDate", "2023-01-01");
        setData.put("MyTimestamp", "2023-08-07 00:00:00");
        setData.put("MyIntervalYearToMonth", "2-6");
        setData.put("MyIntervalDayToSecond", "2 11:00:15");
        setData.put("MyXml", "<root><node>XMLType_123</node></root>");
        setData.put("MyReal", null);

        Map<String, Object> whereData = new HashMap<>();
        whereData.put("MyInteger", 123);
        whereData.put("MyFloat", 1.11);

        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "M", "MYTABLE", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "M", "MYTABLE");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(new BigDecimal("321.32"), result.get(0).get("MYDECIMAL"));
        Assertions.assertEquals(String.format("%-50s", "char'50'"), result.get(0).get("MYCHAR"));
        Assertions.assertEquals("2023-01-01 00:00:00.0", result.get(0).get("MYDATE").toString());
        Assertions.assertEquals("2023-08-07 00:00:00.0", result.get(0).get("MYTIMESTAMP").toString());
        Assertions.assertEquals("2-6", result.get(0).get("MYINTERVALYEARTOMONTH").toString());
        Assertions.assertEquals("2 11:0:15.0", result.get(0).get("MYINTERVALDAYTOSECOND").toString());
        Assertions.assertEquals("<root><node>XMLType_123</node></root>", ((XMLType) result.get(0).get("MYXML")).getString());

        Blob blob = (Blob)result.get(0).get("MYBLOB");
        InputStream is = blob.getBinaryStream();
        byte[] bytes = is.readAllBytes();
        Assertions.assertArrayEquals("Masker".getBytes(), bytes);

        Clob clob = (Clob)result.get(0).get("MYCLOB");
        Reader reader = clob.getCharacterStream();
        BufferedReader br = new BufferedReader(reader);
        Assertions.assertEquals("This is a big CLOB", br.readLine());

        setData = new HashMap<>();
        setData.put("MyDecimal", 999.99);
        whereData = new HashMap<>();
        whereData.put("MyInteger", 123);
        whereData.put("MyDecimal", 321.32);
        whereData.put("MyChar", "char'50'");
        whereData.put("MyDate", "2023-01-01");
        whereData.put("MyTimestamp", "2023-08-07 00:00:00");
        whereData.put("MyIntervalYearToMonth", "2-6");
        whereData.put("MyIntervalDayToSecond", "2 11:00:15");
        whereData.put("MyReal", null);

        sql = DBDialectManager.generateUpdateSql(connection, dbType, "M", "MYTABLE", setData, whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "M", "MYTABLE");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(new BigDecimal("999.99"), result.get(0).get("MYDECIMAL"));
    }

    @Test
    void testGenerateDeleteSql() throws SQLException, ClassNotFoundException {
        createTable2(connection, dbType);
        insertData2(connection, dbType);

        Map<String, Object> whereData = new HashMap<>();
        whereData.put("id", 1);
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        String sql = DBDialectManager.generateDeleteSql(connection, dbType, "M", "EMPLOYEES", whereData, true);
        String expectSQL = "DELETE FROM M.EMPLOYEES WHERE id = 1;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(0, result.size());

        insertData2(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("email", "john.doe@example.com");

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "M", "EMPLOYEES", whereData, true);
        expectSQL = "DELETE FROM M.EMPLOYEES WHERE last_name = 'Doe' AND first_name = 'John';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(0, result.size());

        insertData2(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("email", "john.doe@example.com");
        whereData.put("age", 30);

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "M", "EMPLOYEES", whereData, true);
        expectSQL = "DELETE FROM M.EMPLOYEES WHERE first_name = 'John' AND email = 'john.doe@example.com' AND age = 30;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(0, result.size());

        insertData2(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());

        whereData.put("age", 31);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "M", "EMPLOYEES", whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());

        whereData = null;
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "M", "EMPLOYEES", whereData, true);
        Assertions.assertNull(sql);

        tearDown();
        setUp();
        createTable1(connection, dbType);
        insertData(connection, dbType);

        whereData = new HashMap<>();
        whereData.put("MyInteger", 123);
        whereData.put("MyDecimal", 123.45);
        whereData.put("MyChar", "CHAR");
        whereData.put("MyDate", "2023-07-07");
        whereData.put("MyTimestamp", "2023-07-07 00:00:00");
        whereData.put("MyIntervalYearToMonth", "2-5");
        whereData.put("MyIntervalDayToSecond", "2 12:30:15");
        whereData.put("MyReal", 123.45);

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "M", "MYTABLE", whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "M", "MYTABLE");
        Assertions.assertEquals(0, result.size());
    }
}
