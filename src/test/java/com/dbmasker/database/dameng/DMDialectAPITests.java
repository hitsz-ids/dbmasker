package com.dbmasker.database.dameng;

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
import org.apache.tools.ant.taskdefs.Copy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbBlob;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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

class DMDialectAPITests {
    private String driver = "dm.jdbc.driver.DmDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.DM.getDbName();
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
                      mynumber NUMBER(10, 2),
                      myfloat FLOAT,
                      mydouble DOUBLE,
                      mydate DATE,
                      mytime TIME,
                      mytimestamp TIMESTAMP,
                      mychar CHAR(10),
                      mycharacter CHARACTER(10),
                      myvarchar VARCHAR(100),
                      mycharactervarying CHARACTER VARYING(100),
                      mynchar NCHAR(10),
                      mynvarchar NVARCHAR(100),
                      mybinary BINARY(10),
                      myvarbinary VARBINARY(100),
                      myblob BLOB,
                      mytinyint TINYINT,
                      myclob CLOB,
                      mytext TEXT
                 );
                 """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.mytable (
                       myint,  -- myint
                       myinteger,  -- myinteger
                       mysmallint,  -- mysmallint
                       mydecimal,  -- mydecimal
                       mynumber,  -- mynumber
                       myfloat,  -- myfloat
                       mydouble,  -- mydouble
                       mydate,  -- mydate
                       mytime,  -- mytime
                       mytimestamp,  -- mytimestamp
                       mychar,  -- mychar
                       mycharacter,  -- mycharacter
                       myvarchar,  -- myvarchar
                       mycharactervarying,  -- mycharactervarying
                       mynchar,  -- mynchar
                       mynvarchar,  -- mynvarchar
                       mybinary,  -- mybinary
                       myvarbinary,  -- myvarbinary
                       myblob,  -- myblob
                       mytinyint,  -- mytinyint
                       myclob,  -- myclob
                       mytext  -- mytext
                ) VALUES (
                       1,  -- myint
                       2,  -- myinteger
                       3,  -- mysmallint
                       4.56,  -- mydecimal
                       7.89,  -- mynumber
                       10.11,  -- myfloat
                       12.13,  -- mydouble
                       DATE '2023-01-01',  -- mydate
                       TIME '13:14:15',  -- mytime
                       TIMESTAMP '2023-01-01 13:14:15',  -- mytimestamp
                       'char',  -- mychar
                       'character',  -- mycharacter
                       'varchar',  -- myvarchar
                       'character varying',  -- mycharactervarying
                       'nchar',  -- mynchar
                       'nvarchar',  -- mynvarchar
                       HEXTORAW('44424D61736B6572'),  -- mybinary 'DBMasker' in hexadecimal
                       HEXTORAW('44424D61736B6572'),  -- myvarbinary 'DBMasker' in hexadecimal
                       HEXTORAW('44424D61736B6572'),  -- myblob 'DBMasker' in hexadecimal
                       1,  -- mytinyint
                       'This is CLOB data',  -- myclob
                       'text'  -- mytext
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/dameng.properties")) {
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
                DROP SCHEMA MY_SCHEMA CASCADE;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);

        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testFormatData() throws ParseException {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "INT"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "DECIMAL"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "TEXT"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        Assertions.assertEquals("true", dialect.formatData(true, "BOOLEAN"));
        Assertions.assertEquals("TIMESTAMP '2004-10-19 10:23:54'", dialect.formatData("2004-10-19 10:23:54", "TIMESTAMP"));
        Assertions.assertEquals("DATE '2004-10-19'", dialect.formatData("2004-10-19", "DATE"));
        Assertions.assertEquals("TIME '10:23:54'", dialect.formatData("10:23:54", "TIME"));
        SimpleDateFormat dateF = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeF = new SimpleDateFormat("HH:mm:ss");
        SimpleDateFormat timestampF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Assertions.assertEquals("TIMESTAMP '2004-10-19 10:23:54'", dialect.formatData(new Timestamp(timestampF.parse("2004-10-19 10:23:54").getTime()), "TIMESTAMP"));
        Assertions.assertEquals("TIME '10:23:54'", dialect.formatData(new Time(timeF.parse("10:23:54").getTime()), "TIME"));
        Assertions.assertEquals("DATE '2004-10-19'", dialect.formatData(dateF.parse("2004-10-19"), "DATE"));
        // Test BLOB type
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        Assertions.assertEquals("HEXTORAW('01020304')", dialect.formatData(data, "BINARY"));

        String str = "DBMasker";
        Assertions.assertEquals("HEXTORAW('44424d61736b6572')", dialect.formatData(str, "BLOB"));

        Assertions.assertEquals("'CLOB'", dialect.formatData("CLOB", "CLOB"));
    }

    @Test
    void testGenerateInsertSql() throws SQLException {
        insertData(connection, dbType);

        Map<String, Object> data = new HashMap<>();
        data.put("MyInt", 1);
        data.put("MyInteger", 2);
        data.put("MySmallInt", 3);
        data.put("MyDecimal", 4.56);
        data.put("MyNumber", 7.89);
        data.put("MyFloat", 10.11);
        data.put("MyDouble", 12.13);
        data.put("MyDate", "2023-01-01");
        data.put("MyTime", "13:14:15");
        data.put("MyTimestamp", "2023-01-01 13:14:15");
        data.put("MyChar", "char");
        data.put("MyCharacter", "character");
        data.put("MyVarchar", "varchar");
        data.put("MyCharacterVarying", "character varying");
        data.put("MyNchar", "nchar");
        data.put("MyNvarchar", "nvarchar");
        data.put("MyBinary", "DBMasker".getBytes());
        data.put("MyVarbinary", "DBMasker");
        data.put("MyBlob", "DBMasker".getBytes());
        data.put("MyTinyint", true);
        data.put("MyClob", "This is CLOB data");
        data.put("MyText", "text");

        String sql = DBDialectManager.generateInsertSql(connection, dbType, "MY_SCHEMA", "MYTABLE", data);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "MYTABLE");
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(1, result.get(1).get("MYINT"));
        Assertions.assertEquals(2, result.get(1).get("MYINTEGER"));
        Assertions.assertEquals(Short.valueOf("3"), result.get(1).get("MYSMALLINT"));
        Assertions.assertEquals(new BigDecimal("4.56"), result.get(1).get("MYDECIMAL"));
        Assertions.assertEquals(new BigDecimal("7.89"), result.get(1).get("MYNUMBER"));
        Assertions.assertEquals(10.11, result.get(1).get("MYFLOAT"));
        Assertions.assertEquals(12.13, result.get(1).get("MYDOUBLE"));
        Assertions.assertEquals("2023-01-01", result.get(1).get("MYDATE").toString());
        Assertions.assertEquals("13:14:15", result.get(1).get("MYTIME").toString());
        Assertions.assertEquals("2023-01-01 13:14:15.0", result.get(1).get("MYTIMESTAMP").toString());
        Assertions.assertEquals(String.format("%-10s", "char"), result.get(1).get("MYCHAR"));
        Assertions.assertEquals(String.format("%-10s", "character"), result.get(1).get("MYCHARACTER"));
        Assertions.assertEquals("varchar", result.get(1).get("MYVARCHAR"));
        Assertions.assertEquals("character varying", result.get(1).get("MYCHARACTERVARYING"));
        Assertions.assertEquals(String.format("%-10s", "nchar"), result.get(1).get("MYNCHAR"));
        Assertions.assertEquals("nvarchar", result.get(1).get("MYNVARCHAR"));
        Assertions.assertEquals("DBMasker", new String(Arrays.copyOfRange((byte[]) result.get(1).get("MYBINARY"), 0, 8)));
        Assertions.assertEquals("DBMasker", new String((byte[]) result.get(1).get("MYVARBINARY")));

        Assertions.assertEquals(Byte.valueOf((byte) 1), ((byte)result.get(1).get("MYTINYINT")));

        DmdbNClob clob = (DmdbNClob)result.get(1).get("MYCLOB");
        Assertions.assertEquals("This is CLOB data", clob.getSubString(1, (int)clob.length()));

        clob = (DmdbNClob)result.get(1).get("MYTEXT");
        Assertions.assertEquals("text", clob.getSubString(1, (int)clob.length()));

        DmdbBlob blob = (DmdbBlob)result.get(1).get("MYBLOB");
        Assertions.assertEquals("DBMasker", new String(blob.getBytes(1, (int)blob.length())));

        data.put("MyChar", "char'50'");
        data.put("MyBlob", "DBMasker");
        data.put("MyTinyint", 0);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "MY_SCHEMA", "MYTABLE", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "MYTABLE");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(String.format("%-10s", "char'50'"), result.get(2).get("MYCHAR"));
        Assertions.assertEquals(Byte.valueOf((byte) 0), ((byte)result.get(2).get("MYTINYINT")));

        blob = (DmdbBlob)result.get(2).get("MYBLOB");
        Assertions.assertEquals("DBMasker", new String(blob.getBytes(1, (int)blob.length())));

        data.put("MyInt", null);
        data.put("MyTimestamp", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, "MY_SCHEMA", "MYTABLE", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "MYTABLE");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("MYINT"));
        Assertions.assertNull(result.get(3).get("MYTIMESTAMP"));
    }

}
