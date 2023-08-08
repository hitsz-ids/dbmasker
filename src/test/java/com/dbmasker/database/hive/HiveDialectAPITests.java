package com.dbmasker.database.hive;

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

class HiveDialectAPITests {
    private String driver = "org.apache.hive.jdbc.HiveDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.HIVE.getDbName();
    private String version = "v2";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                  CREATE TABLE my_table2 (
                      my_tinyint TINYINT,
                      my_smallint SMALLINT,
                      my_int INT,
                      my_bigint BIGINT,
                      my_float FLOAT,
                      my_double DOUBLE,
                      my_decimal DECIMAL,
                      my_boolean BOOLEAN,
                      my_string STRING,
                      my_varchar VARCHAR(255),
                      my_char CHAR(255),
                      my_timestamp TIMESTAMP,
                      my_date DATE,
                      my_binary BINARY,
                      my_array ARRAY<INT>,
                      my_map MAP<STRING, INT>,
                      my_struct STRUCT<a:STRING, b:INT, c:DOUBLE>,
                      my_uniontype UNIONTYPE<STRING, INT, DOUBLE>
                  ) ROW FORMAT DELIMITED
                  FIELDS TERMINATED BY ','
                  STORED AS TEXTFILE
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO TABLE my_table2
                SELECT
                    1 AS my_tinyint, -- TINYINT
                    2 AS my_smallint, -- SMALLINT
                    3 AS my_int, -- INT
                    4 AS my_bigint, -- BIGINT
                    5.0 AS my_float, -- FLOAT
                    6.0 AS my_double, -- DOUBLE
                    7.0 AS my_decimal, -- DECIMAL
                    TRUE AS my_boolean, -- BOOLEAN
                    'string' AS my_string, -- STRING
                    'varchar' AS my_varchar, -- VARCHAR
                    'char' AS my_char, -- CHAR
                    '2023-07-24 01:02:03' AS my_timestamp, -- TIMESTAMP
                    '2023-07-24' AS my_date, -- DATE
                    'binary data' AS my_binary, -- BINARY
                    ARRAY(1, 2, 3) AS my_array, -- ARRAY
                    MAP('key', 1) AS my_map, -- MAP
                    NAMED_STRUCT('a', 'struct_string', 'b', 1, 'c', CAST(2.0 AS DOUBLE)) AS my_struct, -- STRUCT
                    CREATE_UNION(0, 'union_string', 1, CAST(2.0 AS DOUBLE)) AS my_uniontype -- UNIONTYPE
                FROM (SELECT 1) t
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }


    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/hive.properties")) {
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
        String sql = "DROP TABLE IF EXISTS my_table2";
        DBManager.executeUpdateSQL(connection, dbType, sql);

        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testSQL() throws SQLException {
        createTable(connection, dbType);
//        insertData(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "", "my_table2");
        Assertions.assertTrue(true);
    }

    @Test
    void testFormatData() throws ParseException {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "INT"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "FLOAT"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "VARCHAR"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Assertions.assertEquals("true", dialect.formatData(true, "BOOLEAN"));
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData("2004-10-19 10:23:54", "TIMESTAMP"));
        SimpleDateFormat dateF = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timestampF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Assertions.assertEquals("'2004-10-19 10:23:54'", dialect.formatData(new Timestamp(timestampF.parse("2004-10-19 10:23:54").getTime()), "TIMESTAMP"));
        Assertions.assertEquals("'2004-10-19'", dialect.formatData(dateF.parse("2004-10-19"), "DATE"));
    }

    @Test
    void testGenerateInsertSql() throws SQLException {
        createTable(connection, dbType);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("my_tinyint", 1);
        data.put("my_smallint", 2);
        data.put("my_int", 3);
        data.put("my_bigint", 4);
        data.put("my_float", 5.0);
        data.put("my_double", 6.0);
        data.put("my_decimal", 7.0);
        data.put("my_boolean", true);
        data.put("my_string", "string");
        data.put("my_varchar", "varchar");
        data.put("my_char", "char");
        data.put("my_timestamp", "2023-07-24 01:02:03");
        data.put("my_date", "2023-07-24");
        data.put("my_binary", "binary data");
        data.put("my_array", "ARRAY(1, 2, 3)");
        data.put("my_map", "MAP('key', 1)");
        data.put("my_struct", "NAMED_STRUCT('a', 'struct_string', 'b', 1, 'c', CAST(2.0 AS DOUBLE))");
        data.put("my_uniontype", "CREATE_UNION(0, 'union_string', 1, CAST(2.0 AS DOUBLE))");

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "", "my_table2", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "my_table2");

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("1", result.get(0).get("my_table2.my_tinyint").toString());
        Assertions.assertEquals("2", result.get(0).get("my_table2.my_smallint").toString());
        Assertions.assertEquals(3, result.get(0).get("my_table2.my_int"));
        Assertions.assertEquals(4L, result.get(0).get("my_table2.my_bigint"));
        Assertions.assertEquals("5.0", result.get(0).get("my_table2.my_float").toString());
        Assertions.assertEquals("6.0", result.get(0).get("my_table2.my_double").toString());
        Assertions.assertEquals(new BigDecimal("7"), result.get(0).get("my_table2.my_decimal"));
        Assertions.assertEquals(true, result.get(0).get("my_table2.my_boolean"));
        Assertions.assertEquals("string", result.get(0).get("my_table2.my_string"));
        Assertions.assertEquals("varchar", result.get(0).get("my_table2.my_varchar"));
        Assertions.assertEquals(String.format("%-255s", "char"), result.get(0).get("my_table2.my_char"));
        Assertions.assertEquals("2023-07-24 01:02:03.0", result.get(0).get("my_table2.my_timestamp").toString());
        Assertions.assertEquals("2023-07-24", result.get(0).get("my_table2.my_date").toString());
        Assertions.assertEquals("binary data", new String((byte[])result.get(0).get("my_table2.my_binary")));
        Assertions.assertEquals("[1,2,3]", result.get(0).get("my_table2.my_array"));
        Assertions.assertEquals("{\"key\":1}", result.get(0).get("my_table2.my_map"));
        Assertions.assertEquals("{\"a\":\"struct_string\",\"b\":1,\"c\":2.0}", result.get(0).get("my_table2.my_struct"));
        Assertions.assertEquals("{0:\"union_string\"}", result.get(0).get("my_table2.my_uniontype"));
    }

}
