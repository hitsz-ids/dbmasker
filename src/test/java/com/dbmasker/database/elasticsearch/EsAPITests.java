package com.dbmasker.database.elasticsearch;

import com.dbmasker.api.DBManager;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.database.DbType;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

class EsAPITests {

    private String driver = "org.elasticsearch.xpack.sql.jdbc.EsDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.ELASTICSEARCH.getDbName();
    private String version = "v8";

    private Connection connection;

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/elasticsearch.properties")) {
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
        DBManager.closeConnection(connection);
    }

    @Test
    void testTestConn() throws SQLException, ClassNotFoundException {
        Assertions.assertTrue(DBManager.testConnection(driver, url, username, password));

        // more test cases
        String fakeDriver = "fake.JDBC";
        try {
            DBManager.testConnection(fakeDriver, url, username, password);
            Assertions.fail();
        } catch (ClassNotFoundException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.JDBC_DRIVER_NOT_FOUND_ERROR));
        }

        String fakeUrl = "jdbc:fake:/fake/fake.db";
        try {
            DBManager.testConnection(driver, fakeUrl, username, password);
            Assertions.fail();
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.CONNECTION_ESTABLISHMENT_FAILURE_ERROR));
        }
    }

    @Test
    void testCreateConn() throws SQLException, ClassNotFoundException {
        // Test the connection with the mocked objects
        Connection tmpConnection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertNotNull(tmpConnection);

        DBManager.closeConnection(tmpConnection);
    }

    @Test
    void testCloseConn() throws SQLException, ClassNotFoundException {
        Connection tmpConnection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertNotNull(connection);

        Assertions.assertTrue(DBManager.closeConnection(tmpConnection));

        // more test cases
        Assertions.assertTrue(tmpConnection.isClosed());
        Assertions.assertTrue(DBManager.closeConnection(tmpConnection));

        Assertions.assertFalse(DBManager.closeConnection(null));
    }

    @Test
    void testExecuteUpdateSQL() {
        //not support
    }

    @Test
    void testGetSchema()  {
        //not support
    }

    @Test
    void testGetTables() throws SQLException {
        List<String> tables = DBManager.getTables(connection, dbType, "");
        Assertions.assertTrue(tables.contains("library"));

        // more test cases
        try {
            DBManager.getTables(null, dbType, "");
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR));
        }

        try {
            DBManager.getTables(connection, null, "");
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_CONNECTION_OR_DB_TYPE_ERROR));
        }

        try {
            DBManager.getTables(connection, "fakeDB", "");
            Assertions.fail();
        } catch (UnsupportedOperationException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.UNSUPPORTED_DATABASE_TYPE_ERROR));
        }

        Assertions.assertTrue(DBManager.getTables(connection, dbType, null).contains("library"));
        Assertions.assertTrue(DBManager.getTables(connection, dbType, "fakeSchema").contains("library"));
    }

    @Test
    void testGetMetaData() {
        // not support
    }

    @Test
    void testGetViews() {
        // not support
    }

    @Test
    void testGetTableAttribute() throws SQLException {
        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "", "library");
        Assertions.assertEquals(tableAttributes.size(), 6);

        // more test cases
        Assertions.assertEquals(8, DBManager.getTableAttribute(connection, dbType, null, "employees").size());
        Assertions.assertEquals(8, DBManager.getTableAttribute(connection, dbType, "fakeSchema", "employees").size());

        try {
            DBManager.getTableAttribute(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertTrue(DBManager.getTableAttribute(connection, dbType, null, "fakeTable").isEmpty());

        Assertions.assertTrue(DBManager.getTableAttribute(connection, dbType, null, "").size() >= 8);
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        String sql = """
                SELECT author, name ,page_count, release_date FROM library
                """;
        List<Map<String, Object>> result = DBManager.executeQuerySQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 3);

        List<Map<String, Object>> expectResult = new ArrayList<>();
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("author", "James S.A. Corey");
        expectMap.put("name", "Leviathan Wakes");
        expectMap.put("page_count", Long.valueOf(561));
        expectMap.put("release_date", Timestamp.valueOf("2011-06-02 08:00:00"));
        expectResult.add(expectMap);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("author", "Dan Simmons");
        expectMap1.put("name", "Hyperion");
        expectMap1.put("page_count", Long.valueOf(482));
        expectMap1.put("release_date", Timestamp.valueOf("1989-05-26 09:00:00"));
        expectResult.add(expectMap1);

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("author", "Frank Herbert");
        expectMap2.put("name", "Dune");
        expectMap2.put("page_count", Long.valueOf(604));
        expectMap2.put("release_date", Timestamp.valueOf("1965-06-01 08:00:00"));
        expectResult.add(expectMap2);

        Assertions.assertEquals(expectMap, result.get(0));
        Assertions.assertEquals(expectMap1, result.get(1));
        Assertions.assertEquals(expectMap2, result.get(2));
        Assertions.assertEquals(expectResult, result);

        // more test cases
        try {
            DBManager.executeQuerySQL(connection, dbType, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_SQL_ERROR));
        }

        try {
            DBManager.executeQuerySQL(connection, dbType, "fakeSQL");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        String insert = """
                INSERT INTO employees (first_name, last_name, email, age) 
                VALUES ('Jane', 'Smith', 'jane.smith@example.com', 28);
                """;
        try {
            DBManager.executeQuerySQL(connection, dbType, insert);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testExecuteSQLScript() throws SQLException {
        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees;
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
        List<List<Map<String, Object>>> result = DBManager.executeSQLScript(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 2);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", Long.valueOf(1));
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        expectMap.put("age", Long.valueOf(30));
        expectResult.add(expectMap);

        Assertions.assertEquals(12, result.get(0).size());
        Assertions.assertEquals(expectMap, result.get(0).get(0));
        Assertions.assertEquals(12, result.get(1).size());
        Assertions.assertEquals(expectMap, result.get(1).get(0));

    }

    @Test
    void testExecuteQuerySQLBatch() throws SQLException {
        String sql = """
                SELECT author, name ,page_count, release_date FROM library
                """;
        String sql2 = """
                SELECT author, name ,page_count FROM library
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(sql2);
        List<List<Map<String, Object>>> result = DBManager.executeQuerySQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(result.size(), 2);

        List<Map<String, Object>> expectResult = new ArrayList<>();
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("author", "James S.A. Corey");
        expectMap.put("name", "Leviathan Wakes");
        expectMap.put("page_count", Long.valueOf(561));
        expectMap.put("release_date", Timestamp.valueOf("2011-06-02 08:00:00"));
        expectResult.add(expectMap);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("author", "Dan Simmons");
        expectMap1.put("name", "Hyperion");
        expectMap1.put("page_count", Long.valueOf(482));
        expectMap1.put("release_date", Timestamp.valueOf("1989-05-26 09:00:00"));
        expectResult.add(expectMap1);

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("author", "Frank Herbert");
        expectMap2.put("name", "Dune");
        expectMap2.put("page_count", Long.valueOf(604));
        expectMap2.put("release_date", Timestamp.valueOf("1965-06-01 08:00:00"));
        expectResult.add(expectMap2);

        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        Map<String, Object> expectMap21 = new HashMap<>();
        expectMap21.put("author", "James S.A. Corey");
        expectMap21.put("name", "Leviathan Wakes");
        expectMap21.put("page_count", Long.valueOf(561));
        expectResult2.add(expectMap21);

        Map<String, Object> expectMap22 = new HashMap<>();
        expectMap22.put("author", "Dan Simmons");
        expectMap22.put("name", "Hyperion");
        expectMap22.put("page_count", Long.valueOf(482));
        expectResult2.add(expectMap22);

        Map<String, Object> expectMap23 = new HashMap<>();
        expectMap23.put("author", "Frank Herbert");
        expectMap23.put("name", "Dune");
        expectMap23.put("page_count", Long.valueOf(604));
        expectResult2.add(expectMap23);

        List<List<Map<String, Object>>> expectResultList = new ArrayList<>();
        expectResultList.add(expectResult);
        expectResultList.add(expectResult2);

        Assertions.assertEquals(expectResultList, result);

        // more test cases
        try {
            DBManager.executeQuerySQLBatch(connection, dbType, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_SQL_LIST_ERROR));
        }

        Assertions.assertEquals(new ArrayList<>(), DBManager.executeQuerySQLBatch(connection, dbType, new ArrayList<>()));

        String fakeSQL = "fakeSQL";
        sqlList.add(fakeSQL);

        try {
            DBManager.executeQuerySQLBatch(connection, dbType, sqlList);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        sqlList.remove(2);
        String updateSql = """
                UPDATE employees SET age = 31 WHERE id = 1;
                """;
        sqlList.add(updateSql);
        try {
            DBManager.executeQuerySQLBatch(connection, dbType, sqlList);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testGetTableData() throws SQLException {
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "library");
        List<Map<String, Object>> expectResult = new ArrayList<>();
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("author", "James S.A. Corey");
        expectMap.put("name", "Leviathan Wakes");
        expectMap.put("page_count", Long.valueOf(561));
        expectMap.put("release_date", Timestamp.valueOf("2011-06-02 08:00:00"));
        expectResult.add(expectMap);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("author", "Dan Simmons");
        expectMap1.put("name", "Hyperion");
        expectMap1.put("page_count", Long.valueOf(482));
        expectMap1.put("release_date", Timestamp.valueOf("1989-05-26 09:00:00"));
        expectResult.add(expectMap1);

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("author", "Frank Herbert");
        expectMap2.put("name", "Dune");
        expectMap2.put("page_count", Long.valueOf(604));
        expectMap2.put("release_date", Timestamp.valueOf("1965-06-01 08:00:00"));
        expectResult.add(expectMap2);

        Assertions.assertEquals(result, expectResult);

        // more test cases
        Assertions.assertEquals(expectResult, DBManager.getTableOrViewData(connection, dbType, null, "library"));

        try {
            DBManager.getTableOrViewData(connection, dbType, "fakeSchema", "library");
            Assertions.fail();
        } catch (SQLException e) {
            // pass
        }

        try {
            DBManager.getTableOrViewData(connection, dbType, "", null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        try {
            DBManager.getTableOrViewData(connection, dbType, "", "fakeTable");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testGetDataWithPage() throws SQLException, ClassNotFoundException {
        List<String> columnNames = new ArrayList<>();
        columnNames.add("first_name");
        columnNames.add("last_name");
        columnNames.add("email");
        Map<String, Object> result = DBManager.getDataWithPage(connection, dbType, "", "employees", columnNames, -1, 5);
        Assertions.assertEquals(result.get("totalPages"), 1);

        List<Map<String, Object>> resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        Assertions.assertEquals(resultList.get(0), expectMap);

        result = DBManager.getDataWithPage(connection, dbType, "", "employees", null, -1, 5);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        result = DBManager.getDataWithPage(connection, dbType, "", "employees", null, 1, 0);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        // more test cases
        columnNames = new ArrayList<>();
        columnNames.add("first_name");
        columnNames.add("fake_column");
        try {
            DBManager.getDataWithPage(connection, dbType, "", "employees", columnNames, 1, 5);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.getDataWithPage(connection, dbType, "fakeSchema", "employees", null, 1, 5);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testCommit() {
        // not support
    }

    @Test
    void testRollBack() {
        // not support
    }

    @Test
    void testGetUniqueKey() {
        // not support
    }

    @Test
    void testGetIndex() {
        // not support
    }

    @Test
    void testGetFunc() {
        // not support
    }

    @Test
    void testExecuteFunction(){
        // not support
    }

}
