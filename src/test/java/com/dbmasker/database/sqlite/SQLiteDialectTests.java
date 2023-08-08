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

}
