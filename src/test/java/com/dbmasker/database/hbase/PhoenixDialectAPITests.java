package com.dbmasker.database.hbase;

import com.dbmasker.api.DBDialectManager;
import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.database.DbType;
import com.dbmasker.dialect.Dialect;
import com.dbmasker.dialect.DialectFactory;
import com.dbmasker.utils.Config;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import com.dbmasker.utils.ObfuscationMethod;
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

class PhoenixDialectAPITests {
    private String driver = "org.apache.phoenix.queryserver.client.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.PHOENIX.getDbName();
    private String version = "v1";

    private Connection connection;

    public void createSchema(Connection connection, String dbType) throws SQLException {
        List<String> sqlList = new ArrayList<>();
        String sql1 = "CREATE SCHEMA my_schema";

        String sql2 = """
                CREATE TABLE my_schema.my_table (
                     my_integer INTEGER NOT NULL primary key,
                     my_unsigned_int UNSIGNED_INT,
                     my_bigint BIGINT,
                     my_unsigned_long UNSIGNED_LONG,
                     my_tinyint TINYINT,
                     my_unsigned_tinyint UNSIGNED_TINYINT,
                     my_smallint SMALLINT,
                     my_unsigned_smallint UNSIGNED_SMALLINT,
                     my_float FLOAT,
                     my_unsigned_float UNSIGNED_FLOAT,
                     my_double DOUBLE,
                     my_unsigned_double UNSIGNED_DOUBLE,
                     my_decimal DECIMAL,
                     my_boolean BOOLEAN,
                     my_time TIME,
                     my_date DATE,
                     my_timestamp TIMESTAMP,
                     my_varchar VARCHAR,
                     my_char CHAR(10),
                     my_varbinary VARBINARY,
                     my_binary BINARY(10)
                )
                """;
        sqlList.add(sql1);
        sqlList.add(sql2);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);
    }

    public void createSchema1(Connection connection, String dbType) throws SQLException {
        List<String> sqlList = new ArrayList<>();
        String sql1 = "CREATE SCHEMA my_schema";

        String sql2 = """
                CREATE TABLE my_schema.employees (
                    id INTEGER NOT NULL primary key,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255),
                    age INTEGER
                )
                """;
        sqlList.add(sql1);
        sqlList.add(sql2);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = """
                upsert into my_schema.employees (id, first_name, last_name, email, age)
                values (1, 'John', 'Doe', 'john.doe@example.com', 30)
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                UPSERT INTO my_schema.my_table (
                    my_integer,
                    my_unsigned_int,
                    my_bigint,
                    my_unsigned_long,
                    my_tinyint,
                    my_unsigned_tinyint,
                    my_smallint,
                    my_unsigned_smallint,
                    my_float,
                    my_unsigned_float,
                    my_double,
                    my_unsigned_double,
                    my_decimal,
                    my_boolean,
                    my_time,
                    my_date,
                    my_timestamp,
                    my_varchar,
                    my_char,
                    my_varbinary,
                    my_binary
                ) VALUES (
                    1,   -- my_integer
                    2,   -- my_unsigned_int
                    3,   -- my_bigint
                    4,   -- my_unsigned_long
                    5,   -- my_tinyint
                    6,   -- my_unsigned_tinyint
                    7,   -- my_smallint
                    8,   -- my_unsigned_smallint
                    9.0, -- my_float
                    10.0,-- my_unsigned_float
                    11.0,-- my_double
                    12.0,-- my_unsigned_double
                    13.0,-- my_decimal
                    TRUE,-- my_boolean
                    TO_TIME('12:34:56', 'HH:mm:ss'), -- my_time
                    TO_DATE('2023-05-31', 'yyyy-MM-dd'), -- my_date
                    TO_TIMESTAMP('2023-05-31 12:34:56', 'yyyy-MM-dd HH:mm:ss'), -- my_timestamp
                    'Hello', -- my_varchar
                    'World', -- my_char
                    'Hello World', -- my_varbinary
                    'Hi' -- my_binary
                )
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/hbase.properties")) {
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
        DBManager.setAutoCommit(connection, dbType, true);
    }

    @AfterEach
    public void tearDown() throws SQLException {
        String sql = """
                DROP TABLE IF EXISTS my_schema.my_table
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        sql = """
                DROP TABLE IF EXISTS my_schema.employees
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        String sql1 = "DROP SCHEMA IF EXISTS my_schema";
        DBManager.executeUpdateSQL(connection, dbType, sql1);

        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testSQL() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "MY_SCHEMA", "MY_TABLE");
        Assertions.assertTrue(true);
    }

    @Test
    void testFormatData() throws ParseException {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "INTEGER"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "FLOAT"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "VARCHAR"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Assertions.assertEquals("TRUE", dialect.formatData(true, "BOOLEAN"));
        Assertions.assertEquals("TO_TIMESTAMP('2004-10-19 10:23:54', 'yyyy-MM-dd HH:mm:ss')", dialect.formatData("2004-10-19 10:23:54", "TIMESTAMP"));
        SimpleDateFormat timeF = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat timestampF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Assertions.assertEquals("TO_TIMESTAMP('2004-10-19 10:23:54.0', 'yyyy-MM-dd HH:mm:ss')", dialect.formatData(new Timestamp(timestampF.parse("2004-10-19 10:23:54").getTime()), "TIMESTAMP"));
        Assertions.assertEquals("TO_TIME('10:23:54', 'HH:mm:ss')", dialect.formatData(new Time(timeF.parse("10:23:54").getTime()), "TIME"));
    }

    @Test
    void testGenerateInsertSql() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> data = new HashMap<>();
        data.put("my_integer", 2);
        data.put("my_unsigned_int", 2);
        data.put("my_bigint", 3);
        data.put("my_unsigned_long", 4);
        data.put("my_tinyint", 5);
        data.put("my_unsigned_tinyint", 6);
        data.put("my_smallint", 7);
        data.put("my_unsigned_smallint", 8);
        data.put("my_float", 9.0);
        data.put("my_unsigned_float", 10.0);
        data.put("my_double", 11.0);
        data.put("my_unsigned_double", 12.0);
        data.put("my_decimal", 13.0);
        data.put("my_boolean", true);
        data.put("my_time", "10:23:54");
        data.put("my_date", "2004-10-19");
        data.put("my_timestamp", "2004-10-19 10:23:54");
        data.put("my_varchar", "Hello, World");
        data.put("my_char", "Hello");
        data.put("my_varbinary", "Hello, World");
        data.put("my_binary", "Hi");

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "MY_SCHEMA", "MY_TABLE", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "MY_TABLE");

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(2, result.get(1).get("MY_INTEGER"));
        Assertions.assertEquals(2, result.get(1).get("MY_UNSIGNED_INT"));
        Assertions.assertEquals(3L, result.get(1).get("MY_BIGINT"));
        Assertions.assertEquals(4L, result.get(1).get("MY_UNSIGNED_LONG"));
        Assertions.assertEquals("5", result.get(1).get("MY_TINYINT").toString());
        Assertions.assertEquals("6", result.get(1).get("MY_UNSIGNED_TINYINT").toString());
        Assertions.assertEquals("7", result.get(1).get("MY_SMALLINT").toString());
        Assertions.assertEquals("8", result.get(1).get("MY_UNSIGNED_SMALLINT").toString());
        Assertions.assertEquals(9.0, result.get(1).get("MY_FLOAT"));
        Assertions.assertEquals(10.0, result.get(1).get("MY_UNSIGNED_FLOAT"));
        Assertions.assertEquals(11.0, result.get(1).get("MY_DOUBLE"));
        Assertions.assertEquals(12.0, result.get(1).get("MY_UNSIGNED_DOUBLE"));
        Assertions.assertEquals("13", result.get(1).get("MY_DECIMAL").toString());
        Assertions.assertEquals(true, result.get(1).get("MY_BOOLEAN"));
        Assertions.assertEquals("10:23:54", result.get(1).get("MY_TIME").toString());
        Assertions.assertEquals("2004-10-19", result.get(1).get("MY_DATE").toString());
        Assertions.assertEquals("2004-10-19 10:23:54.0", result.get(1).get("MY_TIMESTAMP").toString());
        Assertions.assertEquals("Hello, World", result.get(1).get("MY_VARCHAR"));
        Assertions.assertEquals(String.format("%-10s", "Hello"), result.get(1).get("MY_CHAR"));
        Assertions.assertEquals("Hello, World", new String((byte[])result.get(1).get("MY_VARBINARY")));
        Assertions.assertEquals("Hi", new String(Arrays.copyOfRange((byte[])result.get(1).get("MY_BINARY"), 0, 2)));

        data.put("my_integer", 3);
        data.put("my_char", "char'50'");
        data.put("my_boolean", "false");
        sql = DBDialectManager.generateInsertSql(connection, dbType, "MY_SCHEMA", "MY_TABLE", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "MY_TABLE");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(String.format("%-10s", "char'50'"), result.get(2).get("MY_CHAR"));
        Assertions.assertEquals(false, result.get(2).get("MY_BOOLEAN"));

        data.put("my_integer", 4);
        data.put("my_bigint", null);
        data.put("my_binary", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "MY_SCHEMA", "MY_TABLE", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "MY_TABLE");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("MY_BIGINT"));

        byte[] expected = new byte[10];
        Assertions.assertArrayEquals(expected, (byte[]) result.get(3).get("MY_BINARY"));
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
        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "MY_SCHEMA", "EMPLOYEES", setData, whereData, true);
        String expectSQL = "UPSERT INTO MY_SCHEMA.EMPLOYEES (ID,age) VALUES\n(1,100);";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(100, result.get(0).get("AGE"));
        Assertions.assertEquals(1, result.get(0).get("ID"));

        whereData = null;
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "MY_SCHEMA", "EMPLOYEES", setData, whereData, true);
        Assertions.assertNull(sql);
    }

    @Test
    void testGenerateUpdateSql1() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("my_bigint", 100L);
        setData.put("my_varchar", "Hello, World");
        setData.put("my_boolean", false);
        setData.put("my_time", "10:23:54");
        setData.put("my_date", "2024-10-19");
        setData.put("my_timestamp", "2024-10-19 10:23:54");
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("my_integer", 1);

        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "MY_SCHEMA", "MY_TABLE", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "MY_TABLE");

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(100L, result.get(0).get("MY_BIGINT"));
        Assertions.assertEquals("Hello, World", result.get(0).get("MY_VARCHAR"));
        Assertions.assertEquals(false, result.get(0).get("MY_BOOLEAN"));
        Assertions.assertEquals("10:23:54", result.get(0).get("MY_TIME").toString());
        Assertions.assertEquals("2024-10-19", result.get(0).get("MY_DATE").toString());
        Assertions.assertEquals("2024-10-19 10:23:54.0", result.get(0).get("MY_TIMESTAMP").toString());
    }

    @Test
    void testGenerateDeleteSql() throws SQLException, ClassNotFoundException {
        createSchema1(connection, dbType);
        insertData1(connection, dbType);

        Map<String, Object> whereData = new HashMap<>();
        whereData.put("id", 1);
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        String sql = DBDialectManager.generateDeleteSql(connection, dbType, "MY_SCHEMA", "EMPLOYEES", whereData, true);
        String expectSQL = "DELETE FROM MY_SCHEMA.EMPLOYEES WHERE id = 1;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "MY_SCHEMA", "EMPLOYEES", whereData, true);
        expectSQL = "DELETE FROM MY_SCHEMA.EMPLOYEES WHERE last_name = 'Doe' AND first_name = 'John';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("email", "john.doe@example.com");
        whereData.put("age", 30);

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "MY_SCHEMA", "EMPLOYEES", whereData, true);
        expectSQL = "DELETE FROM MY_SCHEMA.EMPLOYEES WHERE first_name = 'John' AND email = 'john.doe@example.com' AND age = 30;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());

        whereData.put("age", 31);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "MY_SCHEMA", "EMPLOYEES", whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(1, result.size());

        whereData = null;
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "MY_SCHEMA", "EMPLOYEES", whereData, true);
        Assertions.assertNull(sql);


        tearDown();
        setUp();
        createSchema(connection, dbType);
        insertData(connection, dbType);

        whereData = new HashMap<>();
        whereData.put("my_bigint", 3L);
        whereData.put("my_varchar", "Hello");
        whereData.put("my_boolean", true);
        whereData.put("my_time", "12:34:56");
        whereData.put("my_date", "2023-05-31");
        whereData.put("my_timestamp", "2023-05-31 12:34:56");

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "MY_SCHEMA", "MY_TABLE", whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "MY_TABLE");
        Assertions.assertEquals(0, result.size());

    }

}
