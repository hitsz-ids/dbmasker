package com.dbmasker.database.gbase;

import com.dbmasker.api.DBDialectManager;
import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.database.DbType;
import com.dbmasker.dialect.Dialect;
import com.dbmasker.dialect.DialectFactory;
import com.dbmasker.utils.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sqlite.core.DB;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

class Gbase8aDialectAPITests {
    private String driver = "com.gbase.jdbc.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.GBASE8A.getDbName();
    private String version = "v8";

    private Connection connection;

    public void createSchema(Connection connection, String dbType) throws SQLException {
        String sql = """
                 CREATE SCHEMA IF NOT EXISTS my_schema;
                 """;
        String sql1 = """
                 USE my_schema;
                 """;
        String sql2 = """
                CREATE TABLE my_schema.mytable (
                      mysmallint SMALLINT,
                      myinteger INTEGER,
                      mybigint BIGINT,
                      mydecimal DECIMAL(10, 2),
                      mynumeric NUMERIC(10, 2),
                      myreal REAL,
                      mydouble DOUBLE PRECISION,
                      myfloat FLOAT(10),
                      mychar CHAR(10),
                      myvarchar VARCHAR(100),
                      mytext TEXT,
                      mydate DATE,
                      mytime TIME,
                      mytimestamp TIMESTAMP,
                      myboolean   BOOLEAN,
                      mybinary    BINARY(50),
                      myblob      LONGBLOB
                );
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(sql1);
        sqlList.add(sql2);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);
    }

    public void createSchema1(Connection connection, String dbType) throws SQLException {
        String sql = """
                 CREATE SCHEMA IF NOT EXISTS my_schema;
                 """;
        String sql1 = """
                 USE my_schema;
                 """;
        String sql2 = """
                 CREATE TABLE IF NOT EXISTS my_schema.employees (
                     id int PRIMARY KEY,
                     first_name VARCHAR(255) NOT NULL,
                     last_name varchar(255) NOT NULL,
                     email varchar(255) NOT NULL,
                     age int
                 );
                 """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(sql1);
        sqlList.add(sql2);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);
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
               INSERT INTO my_schema.mytable (
                   mysmallint,
                   myinteger,
                   mybigint,
                   mydecimal,
                   myreal,
                   mydouble,
                   myfloat,
                   mychar,
                   myvarchar,
                   mytext,
                   mydate,
                   mytime,
                   mytimestamp,
                   myboolean,
                   mybinary,
                   myblob
               ) VALUES (
                   10,                            -- mysmallint
                   20,                            -- myinteger
                   30,                            -- mybigint
                   40.12,                         -- mydecimal
                   50.12345,                      -- myreal
                   60.12345678901234,             -- mydouble
                   70.123456,                     -- myfloat
                   'CHAR',                        -- mychar
                   'VARCHAR',                     -- myvarchar
                   'This is a text field',        -- mytext
                   '2023-07-24',                  -- mydate
                   '10:11:12',                    -- mytime
                   '2023-07-24 10:11:12',         -- mytimestamp
                   true,                          -- myboolean
                   X'44424D61736B6572',           -- mybinary
                   X'44424D61736B6572'            -- myblob
               );
               """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/gbase8a.properties")) {
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
        String sql1 = """
                DROP TABLE IF EXISTS mytable;
                """;
        String sql2 = """
                DROP TABLE IF EXISTS employees;
                """;
        String sql = """
                DROP SCHEMA IF EXISTS my_schema;
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql1);
        sqlList.add(sql2);
        sqlList.add(sql);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);

        DBManager.closeConnection(connection);

        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testSQL() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "my_schema", "mytable");
        Assertions.assertTrue(true);
    }

    @Test
    void testFormatData() throws ParseException {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "INT"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "FLOAT"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "TEXT"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Assertions.assertEquals("1", dialect.formatData(true, "BOOLEAN"));
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData("2004-10-19 10:23:54", "TIMESTAMP"));
        SimpleDateFormat dateF = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeF = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat timestampF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData(new Timestamp(timestampF.parse("2004-10-19 10:23:54").getTime()), "TIMESTAMP"));
        Assertions.assertEquals("'10:23:54'", dialect.formatData(new Time(timeF.parse("10:23:54").getTime()), "TIME"));
        Assertions.assertEquals("'2004-10-19'", dialect.formatData(dateF.parse("2004-10-19"), "DATE"));

        // Test BLOB type
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        Assertions.assertEquals("X'01020304'", dialect.formatData(data, "BINARY"));

        String str = "DBMasker";
        Assertions.assertEquals("X'44424d61736b6572'", dialect.formatData(str, "BLOB"));
    }

    @Test
    void testGenerateInsertSql() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> data = new HashMap<>();
        data.put("mysmallint", 10);
        data.put("myinteger", 20);
        data.put("mybigint", 30);
        data.put("mydecimal", 40.12);
        data.put("mynumeric", 50.12345);
        data.put("myreal", 60.12345678901234);
        data.put("mydouble", 70.123456);
        data.put("myfloat", 80.123456);
        data.put("mychar", "CHAR");
        data.put("myvarchar", "VARCHAR");
        data.put("mytext", "This is a text field");
        data.put("mydate", "2023-07-24");
        data.put("mytime", "10:11:12");
        data.put("mytimestamp", "2023-07-24 10:11:12");
        data.put("myboolean", true);
        data.put("mybinary", "DBMasker");
        data.put("myblob", "DBMasker");

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(10, result.get(1).get("mysmallint"));
        Assertions.assertEquals(20, result.get(1).get("myinteger"));
        Assertions.assertEquals(30L, result.get(1).get("mybigint"));
        Assertions.assertEquals(new BigDecimal("40.12"), result.get(1).get("mydecimal"));
        Assertions.assertEquals(new BigDecimal("50.12"), result.get(1).get("mynumeric"));
        Assertions.assertEquals(60.1234567890123, result.get(1).get("myreal"));
        Assertions.assertEquals(70.123456, result.get(1).get("mydouble"));
        Assertions.assertEquals("80.1235", result.get(1).get("myfloat").toString());
        Assertions.assertEquals("CHAR      ", result.get(1).get("mychar"));
        Assertions.assertEquals("VARCHAR", result.get(1).get("myvarchar"));
        Assertions.assertEquals("This is a text field", result.get(1).get("mytext"));
        Assertions.assertEquals("2023-07-24", result.get(1).get("mydate").toString());
        Assertions.assertEquals("10:11:12", result.get(1).get("mytime").toString());
        Assertions.assertEquals("2023-07-24 10:11:12.0", result.get(1).get("mytimestamp").toString());
        Assertions.assertEquals(true, result.get(1).get("myboolean"));
        Assertions.assertEquals("DBMasker", new String(Arrays.copyOfRange((byte[])result.get(1).get("mybinary"), 0, 8)));
        Assertions.assertEquals("DBMasker", new String((byte[])result.get(1).get("myblob")));


        data.put("mychar", "char'50'");
        data.put("myblob", "DBMasker");
        data.put("myboolean", "false");
        sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(String.format("%-10s", "char'50'"), result.get(2).get("mychar"));
        Assertions.assertEquals(false, result.get(2).get("myboolean"));
        Assertions.assertEquals("DBMasker", new String((byte[])result.get(1).get("myblob")));

        data.put("myinteger", null);
        data.put("myblob", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("myinteger"));
        Assertions.assertNull(result.get(3).get("myblob"));
    }

    @Test
    void testGenerateUpdateSql() throws SQLException {
        createSchema1(connection, dbType);
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
        createSchema(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("myinteger", 100);
        setData.put("myvarchar", "varchar'50'");
        setData.put("mytext", "this is an other text");
        setData.put("mydate", "2024-07-24");
        setData.put("mytimestamp", "2024-07-24 11:12:13");
        setData.put("myboolean", "false");
        setData.put("myblob", "Masker");
        setData.put("mychar", null);
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("mysmallint", 10);

        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "mytable", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(100, result.get(0).get("myinteger"));
        Assertions.assertEquals("varchar'50'", result.get(0).get("myvarchar"));
        Assertions.assertEquals("this is an other text", result.get(0).get("mytext"));
        Assertions.assertEquals("2024-07-24", result.get(0).get("mydate").toString());
        Assertions.assertEquals("2024-07-24 11:12:13.0", result.get(0).get("mytimestamp").toString());
        Assertions.assertEquals(false, result.get(0).get("myboolean"));
        Assertions.assertEquals("Masker", new String((byte[])result.get(0).get("myblob")));
        Assertions.assertNull(result.get(0).get("mychar"));

        setData = new HashMap<>();
        setData.put("myinteger", 999);
        whereData = new HashMap<>();
        whereData.put("myinteger", 100);
        whereData.put("myvarchar", "varchar'50'");
        whereData.put("mytext", "this is an other text");
        whereData.put("mydate", "2024-07-24");
        whereData.put("mytimestamp", "2024-07-24 11:12:13");
        whereData.put("myboolean", "false");
        whereData.put("mychar", null);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "mytable", setData, whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(999, result.get(0).get("myinteger"));
    }


    @Test
    void testGenerateDeleteSql() throws SQLException, ClassNotFoundException {
        createSchema1(connection, dbType);
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
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "employees", whereData, true);
        expectSQL = "DELETE FROM my_schema.employees WHERE last_name = 'Doe' AND first_name = 'John';";
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
        expectSQL = "DELETE FROM my_schema.employees WHERE first_name = 'John' AND email = 'john.doe@example.com' AND age = 30;";
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

        tearDown();
        setUp();
        createSchema(connection, dbType);
        insertData(connection, dbType);

        whereData = new HashMap<>();
        whereData.put("myinteger", 20);
        whereData.put("myvarchar", "VARCHAR");
        whereData.put("mytext", "This is a text field");
        whereData.put("mydate", "2023-07-24");
        whereData.put("mytimestamp", "2023-07-24 10:11:12");
        whereData.put("myboolean", "true");
        whereData.put("mychar", "CHAR");
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "mytable", whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(0, result.size());
    }
}
