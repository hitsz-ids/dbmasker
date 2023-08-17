package com.dbmasker.database.kingbase;

import com.dbmasker.api.DBDialectManager;
import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.data.SensitiveColumn;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.database.DbType;
import com.dbmasker.dialect.Dialect;
import com.dbmasker.dialect.DialectFactory;
import com.dbmasker.utils.Config;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import com.dbmasker.utils.ObfuscationMethod;
import dm.jdbc.driver.DmdbBlob;
import dm.jdbc.driver.DmdbNClob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

class KingBaseDialectAPITests {

    private String driver = "com.kingbase8.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.KINGBASE.getDbName();
    private String version = "v8";

    private Connection connection;

    public void createSchema(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE SCHEMA my_schema;
                CREATE TABLE my_schema.mytable (
                      myint INT,
                      myinteger INTEGER,
                      mysmallint SMALLINT,
                      mydecimal DECIMAL(10, 2),
                      myfloat REAL,
                      mydouble DOUBLE PRECISION,
                      mydate DATE,
                      mytime TIME,
                      mytimestamp TIMESTAMP,
                      mycharacter CHARACTER(10),
                      mycharactervarying CHARACTER VARYING(100),
                      myblob BYTEA,
                      mytext TEXT,
                      myserial SERIAL,
                      mytswtz TIMESTAMP WITH TIME ZONE,
                      mytimetz TIME WITH TIME ZONE,
                      myinterval INTERVAL,
                      myboolean BOOLEAN
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createSchema1(Connection connection, String dbType) throws SQLException {
        String sql = """
                 CREATE SCHEMA my_schema;
                 CREATE TABLE my_schema.employees (
                     id SERIAL PRIMARY KEY,
                     first_name TEXT NOT NULL,
                     last_name TEXT NOT NULL,
                     email TEXT NOT NULL,
                     age INTEGER,
                     UNIQUE (first_name, last_name)
                 );
                 """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.employees (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.mytable (
                    myint,
                    myinteger,
                    mysmallint,
                    mydecimal,
                    myfloat,
                    mydouble,
                    mydate,
                    mytime,
                    mytimestamp,
                    mycharacter,
                    mycharactervarying,
                    myblob,
                    mytext,
                    myserial,
                    mytswtz,
                    mytimetz,
                    myinterval,
                    myboolean
                ) VALUES (
                    1,  -- myint
                    2,  -- myinteger
                    3,  -- mysmallint
                    123.45,  -- mydecimal
                    1.23,  -- myfloat
                    3.14,  -- mydouble
                    '2023-07-24',  -- mydate
                    '12:34:56',  -- mytime
                    '2023-07-24 12:34:56',  -- mytimestamp
                    'CHAR10',  -- mycharacter
                    'VAR100',  -- mycharactervarying
                    E'\\\\x44424D61736B6572',  -- myblob
                    'This is some text',  -- mytext
                    DEFAULT,  -- myserial
                    '2023-07-24 12:34:56+08',  -- mytswtz
                    '12:34:56+08',  -- mytimetz
                    '1 year 2 months 3 days 4 hours 5 minutes 6 seconds',  -- myinterval
                    TRUE  -- myboolean
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }


    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/kingbase.properties")) {
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
        String sql = "DROP TABLE IF EXISTS employees; DROP SCHEMA IF EXISTS my_schema CASCADE;";
        DBManager.executeUpdateSQL(connection, dbType, sql);

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

        Assertions.assertEquals("123", dialect.formatData(123, "INT4"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "FLOAT8"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "TEXT"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Assertions.assertEquals("true", dialect.formatData(true, "BOOLEAN"));
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData("2004-10-19 10:23:54", "TIMESTAMP"));
        SimpleDateFormat dateF = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeF = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat timestampF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData(new Timestamp(timestampF.parse("2004-10-19 10:23:54").getTime()), "TIMESTAMP"));
        Assertions.assertEquals("'10:23:54'", dialect.formatData(new Time(timeF.parse("10:23:54").getTime()), "TIME"));
        Assertions.assertEquals("'2004-10-19'", dialect.formatData(dateF.parse("2004-10-19"), "DATE"));

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
        data.put("MyInt", 1);
        data.put("MyInteger", 2);
        data.put("MySmallint", 3);
        data.put("MyDecimal", 4.56);
        data.put("MyFloat", 10.11);
        data.put("MyDouble", 12.13);
        data.put("MyDate", "2023-01-01");
        data.put("MyTime", "13:14:15");
        data.put("MyTimestamp", "2023-01-01 13:14:15");
        data.put("MyCharacter", "character");
        data.put("MyCharacterVarying", "character varying");
        data.put("MyBlob", "DBMasker".getBytes());
        data.put("MyText", "text");
        data.put("MySerial", "DEFAULT");
        data.put("MyTswtz", "2023-01-01 13:14:15+08");
        data.put("MyTimetz", "13:14:15+08");
        data.put("MyInterval", "1 year 2 months 3 days 4 hours 5 minutes 6 seconds");
        data.put("MyBoolean", true);

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(1, result.get(1).get("myint"));
        Assertions.assertEquals(2, result.get(1).get("myinteger"));
        Assertions.assertEquals(3, result.get(1).get("mysmallint"));
        Assertions.assertEquals(new BigDecimal("4.56"), result.get(1).get("mydecimal"));
        Assertions.assertEquals(Float.valueOf("10.11"), result.get(1).get("myfloat"));
        Assertions.assertEquals(12.13, result.get(1).get("mydouble"));
        Assertions.assertEquals("2023-01-01 00:00:00.0", result.get(1).get("mydate").toString());
        Assertions.assertEquals("13:14:15", result.get(1).get("mytime").toString());
        Assertions.assertEquals("2023-01-01 13:14:15.0", result.get(1).get("mytimestamp").toString());
        Assertions.assertEquals("character ", result.get(1).get("mycharacter"));
        Assertions.assertEquals("character varying", result.get(1).get("mycharactervarying"));
        Assertions.assertEquals("DBMasker", new String((byte[])result.get(1).get("myblob")));
        Assertions.assertEquals("text", result.get(1).get("mytext"));
        Assertions.assertEquals(2, result.get(1).get("myserial"));
        Assertions.assertEquals("2023-01-01 13:14:15.0", result.get(1).get("mytswtz").toString());
        Assertions.assertEquals("13:14:15", result.get(1).get("mytimetz").toString());
        Assertions.assertEquals("1 years 2 mons 3 days 4 hours 5 mins 6.0 secs", result.get(1).get("myinterval").toString());
        Assertions.assertEquals(true, result.get(1).get("myboolean"));


        data.put("MyCharacter", "char'50'");
        data.put("MyBlob", "DBMasker");
        data.put("MyBoolean", "false");
        sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(String.format("%-10s", "char'50'"), result.get(2).get("mycharacter"));
        Assertions.assertEquals(false, result.get(2).get("myboolean"));

        data.put("MyInt", null);
        data.put("MyTimestamp", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "my_schema", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("MYINT"));
        Assertions.assertNull(result.get(3).get("MYTIMESTAMP"));
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
        createSchema(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("myint", 100);
        setData.put("mycharactervarying", "varchar'000'");
        setData.put("mytimestamp", "2024-01-01 13:14:15.0");
        setData.put("mydate", "2024-01-01");
        setData.put("myBlob", "Masker");
        setData.put("myBoolean", false);
        setData.put("myinteger", null);
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("mysmallint", 3);

        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "mytable", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(100, result.get(0).get("myint"));
        Assertions.assertEquals("varchar'000'", result.get(0).get("mycharactervarying"));
        Assertions.assertEquals("2024-01-01 13:14:15.0", result.get(0).get("mytimestamp").toString());
        Assertions.assertEquals("2024-01-01 00:00:00.0", result.get(0).get("mydate").toString());
        Assertions.assertEquals("Masker", new String((byte[])result.get(0).get("myblob")));
        Assertions.assertEquals(false, result.get(0).get("myboolean"));
        Assertions.assertNull(result.get(0).get("myinteger"));

        setData = new HashMap<>();
        setData.put("mysmallint", 999);
        whereData = new HashMap<>();
        whereData.put("myint", 100);
        whereData.put("mycharactervarying", "varchar'000'");
        whereData.put("mytimestamp", "2024-01-01 13:14:15.0");
        whereData.put("mydate", "2024-01-01");
        whereData.put("myBlob", "Masker");
        whereData.put("myBoolean", false);
        whereData.put("myinteger", null);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "my_schema", "mytable", setData, whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(999, result.get(0).get("mysmallint"));
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
        whereData.put("email", "john.doe@example.com");

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
        whereData.put("myint", 1);
        whereData.put("mycharactervarying", "VAR100");
        whereData.put("mytimestamp", "2023-07-24 12:34:56");
        whereData.put("mydate", "2023-07-24");
        whereData.put("myBlob", "DBMasker");
        whereData.put("myBoolean", true);
        whereData.put("myinteger", 2);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "my_schema", "mytable", whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "mytable");
        Assertions.assertEquals(0, result.size());
    }
}
