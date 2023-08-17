package com.dbmasker.database.gbase;

import com.dbmasker.api.DBDialectManager;
import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.data.SensitiveColumn;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.database.DbType;
import com.dbmasker.dialect.Dialect;
import com.dbmasker.dialect.DialectFactory;
import com.dbmasker.dialect.gbase.Gbase8sDialect;
import com.dbmasker.utils.Config;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import com.dbmasker.utils.ObfuscationMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

class Gbase8sDialectAPITests {
    private String driver = "com.gbasedbt.jdbc.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.GBASE8S.getDbName();
    private String version = "v8";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE my_table (
                    my_bigint BIGINT,
                    my_bigserial BIGSERIAL,
                    my_blob BLOB,
                    my_boolean BOOLEAN,
                    my_byte BYTE,
                    my_char CHAR(10),
                    my_clob CLOB,
                    my_date DATE,
                    my_decimal DECIMAL,
                    my_float FLOAT(10),
                    my_int8 INT8,
                    my_datetime DATETIME YEAR TO FRACTION(5),
                    my_interval INTERVAL HOUR TO FRACTION(5),
                    my_set SET(INTEGER NOT NULL),
                    my_list LIST(VARCHAR(255) NOT NULL),
                    my_integer INTEGER,
                    my_nchar NCHAR(10),
                    my_nvarchar NVARCHAR(100),
                    my_smallint SMALLINT,
                    my_smallfloat SMALLFLOAT,
                    my_text TEXT,
                    my_varchar VARCHAR(50),
                    my_serial SERIAL(10)
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createTable1(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE IF NOT EXISTS employees (
                      id SERIAL PRIMARY KEY,
                      first_name VARCHAR(255) NOT NULL,
                      last_name VARCHAR(255) NOT NULL,
                      email VARCHAR(255) NOT NULL,
                      age INT,
                      UNIQUE (email)
                  );
                 """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO employees (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_table (
                    my_bigint,
                    my_bigserial,
                    my_boolean,
                    my_byte,
                    my_blob,
                    my_char,
                    my_clob,
                    my_date,
                    my_decimal,
                    my_float,
                    my_int8,
                    my_datetime,
                    my_interval,
                    my_set,
                    my_list,
                    my_integer,
                    my_nchar,
                    my_nvarchar,
                    my_smallint,
                    my_smallfloat,
                    my_text,
                    my_varchar,
                    my_serial
                ) VALUES (
                    123456789012345678, -- my_bigint
                    1234567890, -- my_bigserial
                    't', -- my_boolean
                    NULL, -- my_byte
                    NULL, -- my_blob
                    'char_data', -- my_char
                    NULL,  -- my_clob
                    '2023-01-01', -- my_date
                    12345.67,    -- my_decimal
                    1.2345, -- my_float
                    12345678, -- my_int8
                    '2023-01-01 01:23:45.67890', -- my_datetime
                    INTERVAL(10:00:00.00000) HOUR TO FRACTION(5), -- my_interval
                    SET{1, 2, 3}, -- my_set
                    LIST{'a', 'b', 'c'}, -- my_list
                    12345, -- my_integer
                    'nchar_data', -- my_nchar
                    'nvarchar_data', -- my_nvarchar
                    1234, -- my_smallint
                    123.45, -- my_smallfloat
                    NULL, -- my_text
                    'varchar_data', -- my_varchar
                    1234567890 -- my_serial
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/gbase8s.properties")) {
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
                DROP TABLE IF EXISTS employees;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        String sql2 = """
                DROP TABLE IF EXISTS mytable;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql2);
        String sql3 = """
                DROP TABLE IF EXISTS my_table;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql3);
        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testSQL() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "", "my_table");
        Assertions.assertTrue(true);
    }

    @Test
    void testSetTransform() throws SQLException {
        Assertions.assertEquals("SET{'1','2','3'}", Gbase8sDialect.transformToSet("[1, 2, 3]", "SET"));
        Assertions.assertEquals("SET{'a','b','c'}", Gbase8sDialect.transformToSet("[a, b, c]", "SET"));
        Assertions.assertEquals("LIST{'a','5','6'}", Gbase8sDialect.transformToSet("[a, 5, 6]", "LIST"));
        Assertions.assertEquals("LIST{'4', '5', '6'}", Gbase8sDialect.transformToSet("LIST{'4', '5', '6'}", "LIST"));
        Assertions.assertEquals("SET{'a', 'b', 'c'}", Gbase8sDialect.transformToSet("SET{'a', 'b', 'c'}", "LIST"));
    }

    @Test
    void testFormatData() throws ParseException {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "INT"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "FLOAT"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "VARCHAR"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Assertions.assertEquals("'t'", dialect.formatData(true, "BOOLEAN"));
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData("2004-10-19 10:23:54", "TIMESTAMP"));
        SimpleDateFormat dateF = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeF = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat timestampF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData(new Timestamp(timestampF.parse("2004-10-19 10:23:54").getTime()), "TIMESTAMP"));
        Assertions.assertEquals("'10:23:54'", dialect.formatData(new Time(timeF.parse("10:23:54").getTime()), "TIME"));
        Assertions.assertEquals("'2004-10-19'", dialect.formatData(dateF.parse("2004-10-19"), "DATE"));

        Assertions.assertEquals("SET{'1','2','3'}", dialect.formatData("[1, 2, 3]", "SET"));
        Assertions.assertEquals("LIST{'a','b','c'}", dialect.formatData("[a, b, c]", "LVARCHAR"));
    }

    @Test
    void testGenerateInsertSql() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> data = new HashMap<>();
        data.put("my_bigint", 123456789012345678L);
        data.put("my_bigserial", 1234567890L);
        data.put("my_boolean", true);
        data.put("my_byte", null);
        data.put("my_blob", null);
        data.put("my_char", "char_data");
        data.put("my_clob", null);
        data.put("my_date", "2023-01-01");
        data.put("my_decimal", 12345.67);
        data.put("my_float", 1.2345);
        data.put("my_int8", 12345678);
        data.put("my_datetime", "2023-01-01 01:23:45.67890");
        data.put("my_interval", "INTERVAL(10:00:00.00000) HOUR TO FRACTION(5)");
        data.put("my_set", "SET{'1', '2', '3'}");
        data.put("my_list", "LIST{'a', 'b', 'c'}");
        data.put("my_integer", 12345);
        data.put("my_nchar", "nchar_data");
        data.put("my_nvarchar", "nvarchar_data");
        data.put("my_smallint", 1234);
        data.put("my_smallfloat", 123.45);
        data.put("my_text", null);
        data.put("my_varchar", "varchar_data");
        data.put("my_serial", 1234567890);

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "", "my_table", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "my_table");

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(123456789012345678L, result.get(0).get("my_bigint"));
        Assertions.assertEquals(1234567890L, result.get(0).get("my_bigserial"));
        Assertions.assertEquals(true, result.get(0).get("my_boolean"));
        Assertions.assertNull(result.get(0).get("my_byte"));
        Assertions.assertNull(result.get(0).get("my_blob"));
        Assertions.assertEquals("char_data ", result.get(0).get("my_char"));
        Assertions.assertNull(result.get(0).get("my_clob"));
        Assertions.assertEquals("2023-01-01", result.get(0).get("my_date").toString());
        Assertions.assertEquals(new BigDecimal("12345.67"), result.get(0).get("my_decimal"));
        Assertions.assertEquals(1.2345, result.get(0).get("my_float"));
        Assertions.assertEquals(12345678L, result.get(0).get("my_int8"));
        Assertions.assertEquals("2023-01-01 01:23:45.6789", result.get(0).get("my_datetime").toString());
        Assertions.assertEquals("10:00:00.00000", result.get(0).get("my_interval").toString());
        Assertions.assertEquals("[1, 2, 3]", result.get(0).get("my_set").toString());
        Assertions.assertEquals("[a, b, c]", result.get(0).get("my_list").toString());
        Assertions.assertEquals(12345, result.get(0).get("my_integer"));
        Assertions.assertEquals("nchar_data", result.get(0).get("my_nchar"));
        Assertions.assertEquals("nvarchar_data", result.get(0).get("my_nvarchar"));
        Assertions.assertEquals("1234", result.get(0).get("my_smallint").toString());
        Assertions.assertEquals("123.45", result.get(0).get("my_smallfloat").toString());
        Assertions.assertNull(result.get(0).get("my_text"));
        Assertions.assertEquals("varchar_data", result.get(0).get("my_varchar"));
        Assertions.assertEquals(1234567890, result.get(0).get("my_serial"));


        data.put("my_char", "char'50'");
        data.put("my_boolean", 0);
        data.put("my_set", "[1, 2, 3]");
        data.put("my_list", "[a, b, c]");
        sql = DBDialectManager.generateInsertSql(connection, dbType, "", "my_table", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "", "my_table");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(String.format("%-10s", "char'50'"), result.get(2).get("my_char"));
        Assertions.assertEquals(false, result.get(2).get("my_boolean"));
        Assertions.assertEquals("[1, 2, 3]", result.get(0).get("my_set").toString());
        Assertions.assertEquals("[a, b, c]", result.get(0).get("my_list").toString());

        data.put("my_integer", null);
        data.put("my_date", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "", "my_table", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "", "my_table");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("my_integer"));
        Assertions.assertNull(result.get(3).get("my_date"));
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
        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "", "employees", setData, whereData, true);
        String expectSQL = "UPDATE employees SET age = 100  WHERE id = 1;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(100, result.get(0).get("age"));
        Assertions.assertEquals(1, result.get(0).get("id"));

        setData.put("age", 10);
        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("email", "john.doe@example.com");
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "", "employees", setData, whereData, true);
        expectSQL = "UPDATE employees SET age = 10  WHERE email = 'john.doe@example.com';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(10, result.get(0).get("age"));
        Assertions.assertEquals(1, result.get(0).get("id"));

        setData.put("age", 99);
        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("age", 10);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "", "employees", setData, whereData, true);
        expectSQL = "UPDATE employees SET age = 99  WHERE last_name = 'Doe' AND first_name = 'John' AND age = 10;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(99, result.get(0).get("age"));
        Assertions.assertEquals(1, result.get(0).get("id"));

        setData.put("age", 98);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "", "employees", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(99, result.get(0).get("age"));

        whereData = null;
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "", "employees", setData, whereData, true);
        Assertions.assertNull(sql);
    }

    @Test
    void testGenerateUpdateSql1() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("my_char", "char'50'");
        setData.put("my_boolean", 0);
        setData.put("my_set", "[10, 20, 30]");
        setData.put("my_decimal", 1.1);
        setData.put("my_list", "[aa, bb, cc]");
        setData.put("my_date", "2024-01-01");
        setData.put("my_datetime", "2024-01-01 00:00:00");
        setData.put("my_integer", null);
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("my_smallint", 1234);

        String sql = DBDialectManager.generateUpdateSql(connection, dbType, "", "my_table", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "my_table");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(String.format("%-10s", "char'50'"), result.get(0).get("my_char"));
        Assertions.assertEquals(false, result.get(0).get("my_boolean"));
        Assertions.assertEquals("[20, 10, 30]", result.get(0).get("my_set").toString());
        Assertions.assertEquals("1.1", result.get(0).get("my_decimal").toString());
        Assertions.assertEquals("[aa, bb, cc]", result.get(0).get("my_list").toString());
        Assertions.assertEquals("2024-01-01", result.get(0).get("my_date").toString());
        Assertions.assertEquals("2024-01-01 00:00:00.0", result.get(0).get("my_datetime").toString());
        Assertions.assertNull(result.get(0).get("my_integer"));

        setData = new HashMap<>();
        setData.put("my_smallint", 999);
        whereData = new HashMap<>();
        whereData.put("my_char", "char'50'");
        whereData.put("my_boolean", 0);
        whereData.put("my_set", "[10, 20, 30]");
        whereData.put("my_decimal", 1.1);
        whereData.put("my_list", "[aa, bb, cc]");
        whereData.put("my_date", "2024-01-01");
        whereData.put("my_datetime", "2024-01-01 00:00:00");
        whereData.put("my_integer", null);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, "", "my_table", setData, whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "", "my_table");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("999", result.get(0).get("my_smallint").toString());
    }

    @Test
    void testGenerateDeleteSql() throws SQLException, ClassNotFoundException {
        createTable1(connection, dbType);
        insertData1(connection, dbType);

        Map<String, Object> whereData = new HashMap<>();
        whereData.put("id", 1);
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        String sql = DBDialectManager.generateDeleteSql(connection, dbType, "", "employees", whereData, true);
        String expectSQL = "DELETE FROM employees WHERE id = 1;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("email", "john.doe@example.com");
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "", "employees", whereData, true);
        expectSQL = "DELETE FROM employees WHERE email = 'john.doe@example.com';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("first_name", "John");
        whereData.put("last_name", "Doe");
        whereData.put("age", 30);

        sql = DBDialectManager.generateDeleteSql(connection, dbType, "", "employees", whereData, true);
        expectSQL = "DELETE FROM employees WHERE last_name = 'Doe' AND first_name = 'John' AND age = 30;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(0, result.size());

        insertData1(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(1, result.size());

        whereData.put("age", 31);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "", "employees", whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Assertions.assertEquals(1, result.size());

        whereData = null;
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "", "employees", whereData, true);
        Assertions.assertNull(sql);

        createTable(connection, dbType);
        insertData(connection, dbType);

        whereData = new HashMap<>();
        whereData.put("my_char", "char_data");
        whereData.put("my_boolean", 1);
        whereData.put("my_set", "[1, 2, 3]");
        whereData.put("my_decimal", 12345.67);
        whereData.put("my_list", "[a, b, c]");
        whereData.put("my_date", "2023-01-01");
        whereData.put("my_datetime", "2023-01-01 01:23:45.67890");
        sql = DBDialectManager.generateDeleteSql(connection, dbType, "", "my_table", whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, "", "my_table");
        Assertions.assertEquals(0, result.size());
    }
}
