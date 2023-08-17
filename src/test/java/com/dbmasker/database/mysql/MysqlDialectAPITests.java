package com.dbmasker.database.mysql;

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
import org.mariadb.jdbc.MariaDbBlob;
import org.sqlite.core.DB;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;

class MysqlDialectAPITests {
    private String driver = "com.mysql.cj.jdbc.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.MYSQL.getDbName();
    private String version = "v8";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE mytable (
                    mybit BIT(1),
                    mytinyint TINYINT,
                    mybool BOOLEAN,
                    mysmallint SMALLINT,
                    mymediumint MEDIUMINT,
                    myint INT,
                    mybigint BIGINT,
                    mydecimal DECIMAL(10, 2),
                    myfloat FLOAT,
                    mydouble DOUBLE,
                    mydate DATE,
                    mydatetime DATETIME,
                    mytimestamp TIMESTAMP,
                    mytime TIME,
                    myyear YEAR,
                    mychar CHAR(10),
                    myvarchar VARCHAR(100),
                    mybinary BINARY(10),
                    myvarbinary VARBINARY(100),
                    mytinyblob TINYBLOB,
                    myblob BLOB,
                    mymediumblob MEDIUMBLOB,
                    mylongblob LONGBLOB,
                    mytinytext TINYTEXT,
                    mytext TEXT,
                    mymediumtext MEDIUMTEXT,
                    mylongtext LONGTEXT,
                    myenum ENUM('option1', 'option2'),
                    myset SET('option1', 'option2', 'option3')
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createTable1(Connection connection, String dbType) throws SQLException {
        String sql = """
                 CREATE TABLE employees (
                    id INT PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    age INT,
                    UNIQUE(last_name, first_name)
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO mytable (
                       mybit, mytinyint, mybool, mysmallint, mymediumint, myint,\s
                       mybigint, mydecimal, myfloat, mydouble, mydate, mydatetime,\s
                       mytimestamp, mytime, myyear, mychar, myvarchar, mybinary,\s
                       myvarbinary, mytinyblob, myblob, mymediumblob, mylongblob,\s
                       mytinytext, mytext, mymediumtext, mylongtext, myenum, myset
                ) VALUES (
                       b'1',        -- mybit
                       1,           -- mytinyint
                       TRUE,        -- mybool
                       10,          -- mysmallint
                       100,         -- mymediumint
                       1000,        -- myint
                       10000,       -- mybigint
                       123.45,      -- mydecimal
                       123.45,      -- myfloat
                       123.45,      -- mydouble
                       '2023-07-24',-- mydate
                       '2023-07-24 14:00:00', -- mydatetime
                       CURRENT_TIMESTAMP,     -- mytimestamp
                       '14:00:00',  -- mytime
                       2023,        -- myyear
                       'char',      -- mychar
                       'varchar',   -- myvarchar
                       0x44424D61736B65722020,  -- mybinary
                       0x44424D61736B65722020,  -- myvarbinary
                       'tinyblob',  -- mytinyblob
                       'blob',      -- myblob
                       'mediumblob',-- mymediumblob
                       'longblob',  -- mylongblob
                       'tinytext',  -- mytinytext
                       'text',      -- mytext
                       'mediumtext',-- mymediumtext
                       'longtext',  -- mylongtext
                       'option1',   -- myenum
                       'option1,option2' -- myset
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO employees (id, first_name, last_name, email, age)
                VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30);
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/mysql.properties")) {
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
                DROP TABLE IF EXISTS mytable;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        sql = """
                DROP TABLE IF EXISTS employees;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);

        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testFormatData() {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "INT"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "DECIMAL"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "TEXT"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Assertions.assertEquals("true", dialect.formatData(true, "BOOLEAN"));
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData("2004-10-19 10:23:54", "TIMESTAMP"));
        // Test BLOB type
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        Assertions.assertEquals("x'01020304'", dialect.formatData(data, "BINARY"));

        String str = "DBMasker";
        Assertions.assertEquals("x'44424d61736b6572'", dialect.formatData(str, "BLOB"));
    }

    @Test
    void testGenerateInsertSql() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> data = new HashMap<>();
        data.put("MyBit", 1);
        data.put("MyTinyint", 1);
        data.put("MyBool", "false");
        data.put("MySmallint", 10);
        data.put("MyMediumint", 100);
        data.put("MyInt", 1000);
        data.put("MyBigint", 10000);
        data.put("MyDecimal", 123.45);
        data.put("MyFloat", 123.45);
        data.put("MyDouble", 123.45);
        data.put("MyDate", "2023-07-24");
        data.put("MyDatetime", "2023-07-24T14:00");
        data.put("MyTimestamp", "2023-07-24 14:00:00");
        data.put("MyTime", "14:00:00");
        data.put("MyYear", "2023");
        data.put("MyChar", "char");
        data.put("MyVarchar", "varchar");
        data.put("MyBinary", "DBMasker");
        data.put("MyVarbinary", "DBMasker");
        data.put("MyTinyblob", "tinyblob");
        data.put("MyBlob", "blob");
        data.put("MyMediumblob", "mediumblob");
        data.put("MyLongblob", "longblob");
        data.put("MyTinytext", "tinytext");
        data.put("MyText", "text");
        data.put("MyMediumtext", "mediumtext");
        data.put("MyLongtext", "longtext");
        data.put("MyEnum", "option1");
        data.put("MySet", "option1,option2");

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "db_mysql_test", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "mytable");
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(true, result.get(1).get("mybit"));
        Assertions.assertEquals(1, result.get(1).get("mytinyint"));
        Assertions.assertEquals(false, result.get(1).get("mybool"));
        Assertions.assertEquals(10, result.get(1).get("mysmallint"));
        Assertions.assertEquals(100, result.get(1).get("mymediumint"));
        Assertions.assertEquals(1000, result.get(1).get("myint"));
        Assertions.assertEquals(10000L, result.get(1).get("mybigint"));
        Assertions.assertEquals(new BigDecimal("123.45"), result.get(1).get("mydecimal"));
        Assertions.assertEquals(Float.valueOf("123.45"), result.get(1).get("myfloat"));
        Assertions.assertEquals(123.45, result.get(1).get("mydouble"));
        Assertions.assertEquals("2023-07-24", result.get(1).get("mydate").toString());
        Assertions.assertEquals("2023-07-24T14:00", result.get(1).get("mydatetime").toString());
        Assertions.assertEquals("2023-07-24 14:00:00.0", result.get(1).get("mytimestamp").toString());
        Assertions.assertEquals("14:00:00", result.get(1).get("mytime").toString());
        Assertions.assertEquals("char", result.get(1).get("mychar"));
        Assertions.assertEquals("varchar", result.get(1).get("myvarchar"));

        Assertions.assertArrayEquals("DBMasker".getBytes(), Arrays.copyOfRange((byte[])result.get(1).get("mybinary"), 0, 8));
        Assertions.assertArrayEquals("DBMasker".getBytes(), (byte[])result.get(1).get("myvarbinary"));
        Assertions.assertArrayEquals("tinyblob".getBytes(), (byte[])result.get(1).get("mytinyblob"));
        Assertions.assertArrayEquals("blob".getBytes(), (byte[])result.get(1).get("myblob"));
        Assertions.assertArrayEquals("mediumblob".getBytes(), (byte[])result.get(1).get("mymediumblob"));
        Assertions.assertArrayEquals("longblob".getBytes(), (byte[])result.get(1).get("mylongblob"));

        Assertions.assertEquals("tinytext", result.get(1).get("mytinytext").toString());
        Assertions.assertEquals("text", result.get(1).get("mytext").toString());
        Assertions.assertEquals("mediumtext", result.get(1).get("mymediumtext").toString());
        Assertions.assertEquals("longtext", result.get(1).get("mylongtext").toString());
        Assertions.assertEquals("option1", result.get(1).get("myenum").toString());
        Assertions.assertEquals("option1,option2", result.get(1).get("myset").toString());

        Assertions.assertEquals(2023, result.get(1).get("myyear"));

        data.put("MyChar", "char'50'");
        data.put("MyBlob", "DBMasker".getBytes());
        data.put("MyBool", true);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "db_mysql_test", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "mytable");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("char'50'", result.get(2).get("mychar"));
        Assertions.assertEquals(true, result.get(2).get("mybool"));
        Assertions.assertArrayEquals("blob".getBytes(), (byte[])result.get(1).get("myblob"));

        data.put("MyInt", null);
        data.put("MyYear", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "db_mysql_test", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "mytable");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("myint"));
        Assertions.assertNull(result.get(3).get("myyear"));
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
        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "db_mysql_test", "employees", setData, whereData, true);
        String expectSQL = "UPDATE db_mysql_test.employees SET age = 100  WHERE id = 1;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(100, result.get(0).get("age"));
        Assertions.assertEquals(1, result.get(0).get("id"));

        setData.put("age", 10);
        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("email", "john.doe@example.com");
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "db_mysql_test", "employees", setData, whereData, true);
        expectSQL = "UPDATE db_mysql_test.employees SET age = 10  WHERE last_name = 'Doe' AND first_name = 'John';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(10, result.get(0).get("age"));
        Assertions.assertEquals(1, result.get(0).get("id"));

        setData.put("age", 99);
        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("email", "john.doe@example.com");
        whereData.put("age", 10);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "db_mysql_test", "employees", setData, whereData, true);
        expectSQL = "UPDATE db_mysql_test.employees SET age = 99  WHERE first_name = 'John' AND email = 'john.doe@example.com' AND age = 10;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(99, result.get(0).get("age"));
        Assertions.assertEquals(1, result.get(0).get("id"));

        setData.put("age", 98);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "db_mysql_test", "employees", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(99, result.get(0).get("age"));

        whereData = null;
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "db_mysql_test", "employees", setData, whereData, true);
        Assertions.assertNull(sql);
    }

    @Test
    void testGenerateUpdateSql1() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("myint", null);
        setData.put("myyear", 2024);
        setData.put("mychar", "char'51'");
        setData.put("mytinyblob", "tinyblob1".getBytes());
        setData.put("mytimestamp", "2024-01-01 00:00:00");
        setData.put("myfloat", 1.1);
        setData.put("myenum", "option2");

        Map<String, Object> whereData = new HashMap<>();
        whereData.put("mytinyint", 1);

        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "db_mysql_test", "mytable", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertNull(result.get(0).get("myint"));
        Assertions.assertEquals(2024, result.get(0).get("myyear"));
        Assertions.assertEquals("char'51'", result.get(0).get("mychar"));
        Assertions.assertArrayEquals("tinyblob1".getBytes(), (byte[])result.get(0).get("mytinyblob"));

        Assertions.assertEquals("2024-01-01 00:00:00.0", result.get(0).get("mytimestamp").toString());
        Assertions.assertEquals("1.1", result.get(0).get("myfloat").toString());
        Assertions.assertEquals("option2", result.get(0).get("myenum"));

        setData = new HashMap<>();
        setData.put("myint", 999);
        whereData = new HashMap<>();
        whereData.put("myint", null);
        whereData.put("myyear", 2024);
        whereData.put("mychar", "char'51'");
        whereData.put("mytinyblob", "tinyblob1".getBytes());
        whereData.put("mytimestamp", "2024-01-01 00:00:00");
        whereData.put("myenum", "option2");
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "db_mysql_test", "mytable", setData, whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(999, result.get(0).get("myint"));
    }

    @Test
    void testGenerateDeleteSql() throws SQLException, ClassNotFoundException {
        createTable1(connection, dbType);
        insertData1(connection, dbType);

        Map<String, Object> whereData = new HashMap<>();
        whereData.put("id", 1);
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        String sql = DBDialectManager.generateDeleteSql(connection, dbType, "db_mysql_test", "employees", whereData, true);
        String expectSQL = "DELETE FROM db_mysql_test.employees WHERE id = 1;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("email", "john.doe@example.com");

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "db_mysql_test", "employees", whereData, true);
        expectSQL = "DELETE FROM db_mysql_test.employees WHERE last_name = 'Doe' AND first_name = 'John';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("email", "john.doe@example.com");
        whereData.put("age", 30);

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "db_mysql_test", "employees", whereData, true);
        expectSQL = "DELETE FROM db_mysql_test.employees WHERE first_name = 'John' AND email = 'john.doe@example.com' AND age = 30;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(1, result.size());

        whereData.put("age", 31);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "db_mysql_test", "employees", whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = null;
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "db_mysql_test", "employees", whereData, true);
        Assertions.assertNull(sql);

        tearDown();
        setUp();
        createTable(connection, dbType);
        insertData(connection, dbType);

        whereData = new HashMap<>();
        whereData.put("myint", 1000);
        whereData.put("myyear", 2023);
        whereData.put("mychar", "char");
        whereData.put("mytinyblob", "tinyblob".getBytes());
        whereData.put("mydatetime", "2023-07-24 14:00:00");
        whereData.put("myenum", "option1");
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "db_mysql_test", "mytable", whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "mytable");
        Assertions.assertEquals(0, result.size());
    }
}
