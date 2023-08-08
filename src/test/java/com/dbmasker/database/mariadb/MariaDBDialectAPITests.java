package com.dbmasker.database.mariadb;

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

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;

class MariaDBDialectAPITests {
    private String driver = "org.mariadb.jdbc.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.MARIADB.getDbName();
    private String version = "v10";

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

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/mariadb.properties")) {
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
        data.put("MyDatetime", "2023-07-24 14:00");
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

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "mytable");
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(true, result.get(1).get("mybit"));
        Assertions.assertEquals(1, result.get(1).get("mytinyint"));
        Assertions.assertEquals(false, result.get(1).get("mybool"));
        Assertions.assertEquals(10, ((Short)result.get(1).get("mysmallint")).intValue());
        Assertions.assertEquals(100, result.get(1).get("mymediumint"));
        Assertions.assertEquals(1000, result.get(1).get("myint"));
        Assertions.assertEquals(10000L, result.get(1).get("mybigint"));
        Assertions.assertEquals(new BigDecimal("123.45"), result.get(1).get("mydecimal"));
        Assertions.assertEquals(Float.valueOf("123.45"), result.get(1).get("myfloat"));
        Assertions.assertEquals(123.45, result.get(1).get("mydouble"));
        Assertions.assertEquals("2023-07-24", result.get(1).get("mydate").toString());
        Assertions.assertEquals("2023-07-24 14:00:00.0", result.get(1).get("mydatetime").toString());
        Assertions.assertEquals("2023-07-24 14:00:00.0", result.get(1).get("mytimestamp").toString());
        Assertions.assertEquals("14:00:00", result.get(1).get("mytime").toString());
        Assertions.assertEquals("char", result.get(1).get("mychar"));
        Assertions.assertEquals("varchar", result.get(1).get("myvarchar"));

        Assertions.assertArrayEquals("DBMasker".getBytes(), Arrays.copyOfRange((byte[])result.get(1).get("mybinary"), 0, 8));

        MariaDbBlob blob = (MariaDbBlob)result.get(1).get("mytinyblob");
        Assertions.assertArrayEquals("tinyblob".getBytes(), blob.getBytes(1, (int)blob.length()));
        blob = (MariaDbBlob)result.get(1).get("myblob");
        Assertions.assertArrayEquals("blob".getBytes(), blob.getBytes(1, (int)blob.length()));
        blob = (MariaDbBlob)result.get(1).get("mymediumblob");
        Assertions.assertArrayEquals("mediumblob".getBytes(), blob.getBytes(1, (int)blob.length()));
        blob = (MariaDbBlob)result.get(1).get("mylongblob");
        Assertions.assertArrayEquals("longblob".getBytes(), blob.getBytes(1, (int)blob.length()));

        Assertions.assertEquals("tinytext", result.get(1).get("mytinytext").toString());
        Assertions.assertEquals("text", result.get(1).get("mytext").toString());
        Assertions.assertEquals("mediumtext", result.get(1).get("mymediumtext").toString());
        Assertions.assertEquals("longtext", result.get(1).get("mylongtext").toString());
        Assertions.assertEquals("option1", result.get(1).get("myenum").toString());
        Assertions.assertEquals("option1,option2", result.get(1).get("myset").toString());

//        java.sql.Date date = (Date) result.get(1).get("myyear");
//        LocalDate localDate = date.toLocalDate();
//        Assertions.assertEquals(2023, localDate.getYear());
        Assertions.assertEquals(2023, result.get(1).get("myyear"));


        data.put("MyChar", "char'50'");
        data.put("MyBlob", "DBMasker".getBytes());
        data.put("MyBool", true);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "", "mytable");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("char'50'", result.get(2).get("mychar"));
        Assertions.assertEquals(true, result.get(2).get("mybool"));
        blob = (MariaDbBlob)result.get(2).get("myblob");
        Assertions.assertArrayEquals("DBMasker".getBytes(), blob.getBytes(1, (int)blob.length()));


        data.put("MyInt", null);
        data.put("MyYear", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "", "mytable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "", "mytable");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("myint"));
        Assertions.assertNull(result.get(3).get("myyear"));
    }
}
