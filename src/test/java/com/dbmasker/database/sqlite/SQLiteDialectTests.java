package com.dbmasker.database.sqlite;

import com.dbmasker.api.DBDialectManager;
import com.dbmasker.api.DBManager;
import com.dbmasker.database.DbType;
import com.dbmasker.dialect.Dialect;
import com.dbmasker.dialect.DialectFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sqlite.core.DB;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SQLiteDialectTests {

    private String driver = "org.sqlite.JDBC";
    private String url = "jdbc:sqlite:/tmp/db_sqlite_test.db";
    private String username = "";
    private String password = "";
    private String dbType = DbType.SQLITE.getDbName();
    private String version = "v3";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE MyTable
                (
                  MyInteger INTEGER,
                  MyDecimal   REAL,
                  MyDateTime TEXT,
                  MyNull     NULL,
                  MyBlob      BLOB
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createTable2(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE MyTable
                (
                  MyInteger INTEGER PRIMARY KEY,
                  MyDecimal   REAL,
                  MyDateTime TEXT,
                  MyNull     NULL,
                  MyBlob      BLOB NOT NULL,
                  UNIQUE(MyDateTime, MyBlob)
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO MyTable
                (
                 MyInteger,
                 MyDecimal,
                 MyDateTime,
                 MyNull,
                 MyBlob
                )
                VALUES
                (
                 123,                      -- Integer value
                 1234.56,                  -- Numeric value
                 '2023-05-31 12:34:56.789',-- Date and Time
                 NULL,                     -- NULL
                 X'44424D61736B6572'             -- BLOB data, inserting the Hex representation of "DBMasker"
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    @BeforeEach
    public void setUp() throws SQLException, ClassNotFoundException {
        connection = DBManager.createConnection(driver, url, username, password);
    }

    @AfterEach
    public void tearDown() throws SQLException {
        DBManager.closeConnection(connection);

        File file = new File("/tmp/db_sqlite_test.db");
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    void testGenerateInsertSql() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> data = new HashMap<>();
        data.put("MyInteger", 456);
        data.put("MyDecimal", 111.11);
        data.put("MyDateTime", "2023-06-31 12:34:56.789");
        data.put("MyBlob", "DBMasker");
        data.put("MyNull", null);
        String sql = DBDialectManager.generateInsertSql(connection, dbType, null, "MyTable", data);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(456, result.get(1).get("MyInteger"));
        Assertions.assertEquals(111.11, result.get(1).get("MyDecimal"));
        Assertions.assertEquals("2023-06-31 12:34:56.789", result.get(1).get("MyDateTime"));
        Assertions.assertNull(result.get(1).get("MyNull"));
        Assertions.assertArrayEquals("DBMasker".getBytes(), (byte[])result.get(1).get("MyBlob"));

        data.put("MyDateTime", "2023-06-31 '12:34:56.789'");
        data.put("MyBlob", "DBMasker".getBytes());
        sql = DBDialectManager.generateInsertSql(connection, dbType, null, "MyTable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals(456, result.get(2).get("MyInteger"));
        Assertions.assertEquals(111.11, result.get(2).get("MyDecimal"));
        Assertions.assertEquals("2023-06-31 '12:34:56.789'", result.get(2).get("MyDateTime"));
        Assertions.assertNull(result.get(2).get("MyNull"));
        Assertions.assertArrayEquals("DBMasker".getBytes(), (byte[])result.get(2).get("MyBlob"));

        data.put("MyInteger", null);
        data.put("MyDateTime", null);
        sql = DBDialectManager.generateInsertSql(connection, dbType, null, "MyTable", data);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");

        Assertions.assertEquals(4, result.size());
        Assertions.assertNull(result.get(3).get("MyInteger"));
        Assertions.assertNull(result.get(3).get("MyDateTime"));
    }

    @Test
    void testFormatData() {
        Dialect dialect = new DialectFactory().getDialect(dbType);

        Assertions.assertEquals("123", dialect.formatData(123, "INTEGER"));
        Assertions.assertEquals("123.45", dialect.formatData(123.45, "REAL"));
        Assertions.assertEquals("'Hello, World!'", dialect.formatData("Hello, World!", "TEXT"));
        Assertions.assertEquals("123", dialect.formatData(123, "UNKNOWN"));
        Assertions.assertEquals("NULL", dialect.formatData(null, "INTEGER"));
        // Test BLOB type
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        Assertions.assertEquals("X'01020304'", dialect.formatData(data, "BLOB"));

        String str = "DBMasker";
        Assertions.assertEquals("X'44424d61736b6572'", dialect.formatData(str, "BLOB"));
    }

    @Test
    void testGenerateUpdateSql() throws SQLException {
        createTable2(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("MyDecimal", 123.123);
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("MyInteger", 123);
        whereData.put("MyDecimal", 1234.56);
        whereData.put("MyDateTime", "2023-05-31 12:34:56.789");
        String sql = DBDialectManager.generateUpdateSql(connection, dbType, null, "MyTable", setData, whereData, true);
        String expectSQL = "UPDATE MyTable SET mydecimal = 123.123  WHERE myinteger = 123;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(123, result.get(0).get("MyInteger"));
        Assertions.assertEquals(123.123, result.get(0).get("MyDecimal"));

        setData.put("MyDecimal", 111.111);
        whereData = new HashMap<>();
        whereData.put("MyBlob", "DBMasker".getBytes());
        whereData.put("MyDecimal", 1234.56);
        whereData.put("MyDateTime", "2023-05-31 12:34:56.789");
        sql = DBDialectManager.generateUpdateSql(connection, dbType, null, "MyTable", setData, whereData, true);
        expectSQL = "UPDATE MyTable SET mydecimal = 111.111  WHERE mydatetime = '2023-05-31 12:34:56.789' AND myblob = X'44424d61736b6572';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(111.111, result.get(0).get("MyDecimal"));

        setData.put("MyDecimal", 222.222);
        whereData = new HashMap<>();
        whereData.put("MyDecimal", 111.111);
        whereData.put("MyDateTime", "2023-05-31 12:34:56.789");
        whereData.put("MyNull", null);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, null, "MyTable", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(222.222, result.get(0).get("MyDecimal"));

        setData.put("MyDecimal", 333.33);
        sql = DBDialectManager.generateUpdateSql(connection, dbType, null, "MyTable", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(222.222, result.get(0).get("MyDecimal"));

        whereData = null;
        sql = DBDialectManager.generateUpdateSql(connection, dbType, null, "MyTable", setData, whereData, true);
        Assertions.assertNull(sql);
    }

    @Test
    void testGenerateUpdateSql1() throws SQLException {
        createTable2(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> setData = new HashMap<>();
        setData.put("MyInteger", 234);
        setData.put("MyDecimal", 22.123);
        setData.put("MyDateTime", "2023-06-31 12:34:56.789");
        setData.put("MyNull", null);
        setData.put("MyBlob", "Masker".getBytes());
        Map<String, Object> whereData = new HashMap<>();
        whereData.put("MyInteger", 123);
        String sql = DBDialectManager.generateUpdateSql(connection, dbType, null, "MyTable", setData, whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(234, result.get(0).get("MyInteger"));
        Assertions.assertEquals(22.123, result.get(0).get("MyDecimal"));
        Assertions.assertEquals("2023-06-31 12:34:56.789", result.get(0).get("MyDateTime").toString());
        Assertions.assertNull(result.get(0).get("MyNull"));
        Assertions.assertEquals("Masker", new String((byte[]) result.get(0).get("MyBlob")));

        setData.put("MyInteger", 345);
        whereData = new HashMap<>();
        whereData.put("MyInteger", 234);
        whereData.put("MyDecimal", 22.123);
        whereData.put("MyDateTime", "2023-06-31 12:34:56.789");
        whereData.put("MyNull", null);
        whereData.put("MyBlob", "Masker".getBytes());
        sql = DBDialectManager.generateUpdateSql(connection, dbType, null, "MyTable", setData, whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(345, result.get(0).get("MyInteger"));
    }

    @Test
    void testGenerateDeleteSql() throws SQLException {
        createTable2(connection, dbType);
        insertData(connection, dbType);

        Map<String, Object> whereData = new HashMap<>();
        whereData.put("MyInteger", 123);
        whereData.put("MyDecimal", 1234.56);
        whereData.put("MyDateTime", "2023-05-31 12:34:56.789");
        String sql = DBDialectManager.generateDeleteSql(connection, dbType, null, "MyTable", whereData, true);
        String expectSQL = "DELETE FROM MyTable WHERE myinteger = 123;";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(0, result.size());

        insertData(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("MyBlob", "DBMasker".getBytes());
        whereData.put("MyDecimal", 1234.56);
        whereData.put("MyDateTime", "2023-05-31 12:34:56.789");
        sql = DBDialectManager.generateDeleteSql(connection, dbType, null, "MyTable", whereData, true);
        expectSQL = "DELETE FROM MyTable WHERE mydatetime = '2023-05-31 12:34:56.789' AND myblob = X'44424d61736b6572';";
        Assertions.assertEquals(expectSQL, sql);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(0, result.size());

        insertData(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());

        whereData = new HashMap<>();
        whereData.put("MyDecimal", 1234.56);
        whereData.put("MyDateTime", "2023-05-31 12:34:56.789");
        whereData.put("MyNull", null);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, null, "MyTable", whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(0, result.size());

        insertData(connection, dbType);
        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());

        whereData.put("MyInteger", 1234);
        sql = DBDialectManager.generateDeleteSql(connection, dbType, null, "MyTable", whereData, true);
        DBManager.executeSQLScript(connection, dbType, sql);
        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(1, result.size());

        whereData = null;
        sql = DBDialectManager.generateDeleteSql(connection, dbType, null, "MyTable", whereData, true);
        Assertions.assertNull(sql);

        whereData = new HashMap<>();
        whereData.put("MyInteger", 123);
        whereData.put("MyDecimal", 1234.56);
        whereData.put("MyDateTime", "2023-05-31 12:34:56.789");
        whereData.put("MyNull", null);
        whereData.put("MyBlob", "DBMasker".getBytes());
        sql = DBDialectManager.generateDeleteSql(connection, dbType, null, "MyTable", whereData, false);
        DBManager.executeSQLScript(connection, dbType, sql);

        result = DBManager.getTableOrViewData(connection, dbType, null, "MyTable");
        Assertions.assertEquals(0, result.size());
    }
}
