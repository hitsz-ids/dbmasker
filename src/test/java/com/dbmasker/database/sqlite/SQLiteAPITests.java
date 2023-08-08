package com.dbmasker.database.sqlite;

import com.dbmasker.api.DBManager;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.data.TableIndex;
import com.dbmasker.data.TableMetaData;
import com.dbmasker.database.DbType;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

class SQLiteAPITests {

    private String driver = "org.sqlite.JDBC";
    private String url = "jdbc:sqlite:/tmp/db_sqlite_test.db";
    private String username = "";
    private String password = "";
    private String dbType = DbType.SQLITE.getDbName();
    private String version = "v3";

    @AfterEach
    public void tearDown() {
        File file = new File("/tmp/db_sqlite_test.db");
        if (file.exists()) {
            file.delete();
        }
    }

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    age INTEGER
                );
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createView(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE VIEW employee_view AS
                SELECT first_name, last_name, email
                FROM employees;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO employees (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO employees (first_name, last_name, email, age) 
                VALUES ('Jane', 'Smith', 'jane.smith@example.com', 28);
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData2(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO employees (first_name, last_name, email, age) VALUES
                   ('Alice', 'Smith', 'alice.smith@example.com', 30),
                   ('Bob', 'Johnson', 'bob.johnson@example.com', 35),
                   ('Charlie', 'Williams', 'charlie.williams@example.com', 28),
                   ('David', 'Brown', 'david.brown@example.com', 42),
                   ('Eva', 'Jones', 'eva.jones@example.com', 26),
                   ('Frank', 'Garcia', 'frank.garcia@example.com', 33),
                   ('Grace', 'Martinez', 'grace.martinez@example.com', 29),
                   ('Hannah', 'Anderson', 'hannah.anderson@example.com', 31),
                   ('Ivan', 'Thomas', 'ivan.thomas@example.com', 27),
                   ('Jane', 'Jackson', 'jane.jackson@example.com', 36);
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
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
        Assertions.assertNotNull(DBManager.createConnection(driver, url, username, password));
    }

    @Test
    void testCloseConn() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertNotNull(connection);

        Assertions.assertTrue(DBManager.closeConnection(connection));

        // more test cases
        Assertions.assertTrue(connection.isClosed());
        Assertions.assertTrue(DBManager.closeConnection(connection));

        Assertions.assertFalse(DBManager.closeConnection(null));
    }

    @Test
    void testGetSchema() throws SQLException, ClassNotFoundException {
        // not support
    }

    @Test
    void testGetTables() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);

        List<String> sqliteTables = DBManager.getTables(connection, dbType, "");
        List<String> expectTables = new ArrayList<>();
        expectTables.add("employees");

        Assertions.assertEquals(expectTables, sqliteTables);

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

        Assertions.assertEquals(expectTables,DBManager.getTables(connection, dbType, null));
        Assertions.assertEquals(expectTables,DBManager.getTables(connection, dbType, "fakeSchema"));

        DBManager.closeConnection(connection);

        tearDown();
        Connection newConnection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getTables(newConnection, dbType, ""));
    }

    @Test
    void testGetViews() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        createView(connection, dbType);

        List<String> sqliteViews = DBManager.getViews(connection, dbType, "");
        List<String> expectViews = new ArrayList<>();
        expectViews.add("employee_view");

        Assertions.assertEquals(expectViews, sqliteViews);

        // more test cases
        Assertions.assertEquals(expectViews, DBManager.getViews(connection, dbType, null));
        Assertions.assertEquals(expectViews, DBManager.getViews(connection, dbType, "fakeSchema"));

        DBManager.closeConnection(connection);
        tearDown();
        Connection newConnection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getViews(newConnection, dbType, ""));
    }

    @Test
    void testGetMetaData() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);

        List<TableMetaData> metaDatas = DBManager.getMetaData(connection, dbType, "");
        Assertions.assertEquals(metaDatas.size(), 5);

        // more test cases
        Assertions.assertEquals(5, DBManager.getMetaData(connection, dbType, null).size());
        Assertions.assertEquals(5, DBManager.getMetaData(connection, dbType, "fakeSchema").size());

        DBManager.closeConnection(connection);
        tearDown();
        Connection newConnection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getMetaData(newConnection, dbType, ""));
        
    }

    @Test
    void testGetTableAttribute() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "", "employees");
        Assertions.assertEquals(tableAttributes.size(), 5);

        // more test cases
        Assertions.assertEquals(5, DBManager.getTableAttribute(connection, dbType, null, "employees").size());
        Assertions.assertEquals(5, DBManager.getTableAttribute(connection, dbType, "fakeSchema", "employees").size());

        try {
            DBManager.getTableAttribute(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }
        try {
            DBManager.getTableAttribute(connection, dbType, null, "fakeTable").size();
            Assertions.fail();
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().startsWith("Table not found:"));
        }
        try {
            DBManager.getTableAttribute(connection, dbType, null, "").size();
            Assertions.fail();
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().startsWith("Invalid table name:"));
        }
    }

    @Test
    void testGetPrimaryKeys() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);

        List<String> tablePrimaryKeys = DBManager.getPrimaryKeys(connection, dbType, "", "employees");
        Assertions.assertEquals(1, tablePrimaryKeys.size());

        List<String> expectPrimaryKeys = new ArrayList<>();
        expectPrimaryKeys.add("id");

        Assertions.assertEquals(expectPrimaryKeys, tablePrimaryKeys);

        // more test cases
        Assertions.assertEquals(1, DBManager.getPrimaryKeys(connection, dbType, null, "employees").size());
        Assertions.assertEquals(1, DBManager.getPrimaryKeys(connection, dbType, "fakeSchema", "employees").size());

        try {
            DBManager.getPrimaryKeys(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        try {
            DBManager.getPrimaryKeys(connection, dbType, null, "fakeTable");
            Assertions.fail();
        } catch (SQLException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.TABLE_NOT_FOUND_ERROR));
        }
    }

    @Test
    void testGetUniqueKey() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);

        Map<String, Set<String>> tableUniqueKey = DBManager.getUniqueKeys(connection, dbType, "", "employees");
        Assertions.assertEquals(tableUniqueKey.size(), 1);

        // more test cases
        Assertions.assertEquals(1, DBManager.getUniqueKeys(connection, dbType, null, "employees").size());
        Assertions.assertEquals(1, DBManager.getUniqueKeys(connection, dbType, "fakeSchema", "employees").size());

        try {
            DBManager.getUniqueKeys(connection, dbType, null, null).size();
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, null, "fakeTable").size());
    }

    @Test
    void testGetIndex() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);

        List<TableIndex> tableIndex = DBManager.getIndex(connection, dbType, "", "employees");
        TableIndex tableIndex1 = new TableIndex("sqlite_autoindex_employees_1", "email", true);
        List<TableIndex> expectIndex = new ArrayList<>();
        expectIndex.add(tableIndex1);
        Assertions.assertEquals(tableIndex.size(), expectIndex.size());

        // more test cases
        Assertions.assertEquals(expectIndex.size(), DBManager.getIndex(connection, dbType, null, "employees").size());
        Assertions.assertEquals(expectIndex.size(), DBManager.getIndex(connection, dbType, "fakeSchema", "employees").size());

        try {
            Assertions.assertEquals(0, DBManager.getIndex(connection, dbType, null, null).size());
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getIndex(connection, dbType, null, "fakeTable").size());
    }

    @Test
    void testExecuteUpdateSQL() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        String sql =  """
                CREATE TABLE employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    age INTEGER
                );
                INSERT INTO employees (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        int rowCount = DBManager.executeUpdateSQL(connection, dbType, sql);
        Assertions.assertEquals(rowCount, 1);

        // more test cases
        try {
            DBManager.executeUpdateSQL(connection, dbType, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_SQL_ERROR));
        }
        try {
            DBManager.executeUpdateSQL(connection, dbType, "fakeSQL");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
        Assertions.assertEquals(0, DBManager.executeUpdateSQL(connection, dbType, "SELECT id, first_name, last_name, email, age FROM employees;"));
    }

    @Test
    void testExecuteUpdateSQLBatch() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        List<String> sqlList = new ArrayList<>();
        String createTable = """
                CREATE TABLE employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    age INTEGER
                );
                """;
        String createView = """
                CREATE VIEW employee_view AS
                SELECT first_name, last_name, email
                FROM employees;
                """;
        String insert = """
                INSERT INTO employees (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        sqlList.add(createTable);
        sqlList.add(createView);
        sqlList.add(insert);

        int rowCount = DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(rowCount, 1);

        // more test cases
        try {
            DBManager.executeUpdateSQLBatch(connection, dbType, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_SQL_LIST_ERROR));
        }

        Assertions.assertEquals(0, DBManager.executeUpdateSQLBatch(connection, dbType, new ArrayList<>()));

        String insert2 = """
                INSERT INTO employees (first_name, last_name, email, age) 
                VALUES ('Jane', 'Smith', 'jane.smith@example.com', 28);
                """;
        String fakeSQL = "fakeSQL";
        ArrayList<String> sqlList2 = new ArrayList<>();
        sqlList2.add(insert2);
        sqlList2.add(fakeSQL);
        try {
            DBManager.executeUpdateSQLBatch(connection, dbType, sqlList2);
            Assertions.fail();
        } catch (SQLException e) {
            Assertions.assertEquals(1, DBManager.getTableOrViewData(connection, dbType, "", "employees").size());
        }
    }

    @Test
    void testExecuteQuerySQL() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
        List<Map<String, Object>> result = DBManager.executeQuerySQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        expectMap.put("age", 30);
        expectResult.add(expectMap);

        Assertions.assertEquals(result, expectResult);

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
    void testExecuteQuerySQLBatch() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        insertData(connection, dbType);
        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
        String sql2 = """
                SELECT first_name, last_name, email FROM employees;
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(sql2);

        List<List<Map<String, Object>>> result = DBManager.executeQuerySQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(result.size(), 2);

        Map<String, Object> expectMap1 = new HashMap<>();
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectMap1.put("last_name", "Doe");
        expectMap1.put("id", 1);
        expectMap1.put("first_name", "John");
        expectMap1.put("email", "john.doe@example.com");
        expectMap1.put("age", 30);
        expectResult1.add(expectMap1);

        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("last_name", "Doe");
        expectMap2.put("first_name", "John");
        expectMap2.put("email", "john.doe@example.com");
        expectResult2.add(expectMap2);

        List<List<Map<String, Object>>> expectResults = new ArrayList<>();
        expectResults.add(expectResult1);
        expectResults.add(expectResult2);

        Assertions.assertEquals(expectResults, result);

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
    void testExecuteSQL() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
        List<Map<String, Object>> result = DBManager.executeSQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        expectMap.put("age", 30);
        expectResult.add(expectMap);

        Assertions.assertEquals(result, expectResult);

        String updateSql = """
                UPDATE employees SET age = 31 WHERE id = 1;
                """;
        result = DBManager.executeSQL(connection, dbType, updateSql);
        Assertions.assertEquals(result.size(), 1);


        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("rows", 1);
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectResult1.add(expectMap1);

        Assertions.assertEquals(result, expectResult1);

        // more test cases
        sql = """
                SELECT id, first_name, last_name, email, age FROM employees;
                UPDATE employees SET age = 32 WHERE id = 1;
                """;

        result = DBManager.executeSQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);
        expectMap.put("age", 31);
        Assertions.assertEquals(result, expectResult);

        sql = """
                UPDATE employees SET age = 32 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
        result = DBManager.executeSQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);
        Assertions.assertEquals(result, expectResult1);

        try {
            DBManager.executeSQL(connection, dbType, "fakeSQL");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testExecuteSQLBatch() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql1 = """
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
        String sql2 = """
                UPDATE employees SET age = 31 WHERE id = 1;
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql1);
        sqlList.add(sql2);

        List<List<Map<String, Object>>> result = DBManager.executeSQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(result.size(), 2);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        expectMap.put("age", 30);
        expectResult.add(expectMap);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("rows", 1);
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectResult1.add(expectMap1);

        List<List<Map<String, Object>>> expectResultList = new ArrayList<>();
        expectResultList.add(expectResult);
        expectResultList.add(expectResult1);

        Assertions.assertEquals(result, expectResultList);

        sql2 = """
                UPDATE employees SET age = 32 WHERE id = 1;
                """;
        sqlList = new ArrayList<>();
        sqlList.add(sql2);
        sqlList.add(sql1);

        result = DBManager.executeSQLBatch(connection, dbType, sqlList);
        expectResultList = new ArrayList<>();
        expectResultList.add(expectResult1);
        expectMap.put("age", 32);
        expectResultList.add(expectResult);

        Assertions.assertEquals(result, expectResultList);
    }

    @Test
    void testExecuteSQLScript() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sqlScript = """
                SELECT id, first_name, last_name, email, age FROM employees;
                UPDATE employees SET age = 31 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM employees;
                """;

        List<List<Map<String, Object>>> result = DBManager.executeSQLScript(connection, dbType, sqlScript);
        Assertions.assertEquals(result.size(), 3);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        expectMap.put("age", 30);
        expectResult.add(expectMap);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("rows", 1);
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectResult1.add(expectMap1);

        Assertions.assertEquals(result.get(0), expectResult);
        Assertions.assertEquals(result.get(1), expectResult1);
        expectMap.put("age", 31);
        Assertions.assertEquals(result.get(2), expectResult);

        sqlScript = """
                UPDATE employees SET age = 31 WHERE id = 1;
                UPDATE employees SET age = 32 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM employees;
                """;

        result = DBManager.executeSQLScript(connection, dbType, sqlScript);
        Assertions.assertEquals(result.size(), 3);

        Assertions.assertEquals(result.get(1), expectResult1);
        Assertions.assertEquals(result.get(0), expectResult1);
        expectMap.put("age", 32);
        Assertions.assertEquals(result.get(2), expectResult);

        // more test cases
        sqlScript = """
                UPDATE employees SET age = 33 WHERE id = 1;
                fakeSQL;
                """;
        try {
            DBManager.executeSQLScript(connection, dbType, sqlScript);
            Assertions.fail();
        } catch (SQLException e) {
            sqlScript = """
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
            result = DBManager.executeSQLScript(connection, dbType, sqlScript);
            Assertions.assertEquals(result.get(0), expectResult);
        }

        try {
            DBManager.executeSQLScript(connection, dbType, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_SQL_SCRIPT_ERROR));
        }

        result = DBManager.executeSQLScript(connection, dbType, ";;");
        Assertions.assertTrue(result.isEmpty());
    }


    @Test
    void testCommit() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        boolean setAutoCommit = DBManager.setAutoCommit(connection, dbType, false);
        Assertions.assertTrue(setAutoCommit);

        insertData(connection, dbType);

        boolean isCommitted = DBManager.commit(connection, dbType);
        Assertions.assertTrue(isCommitted);

        DBManager.setAutoCommit(connection, dbType, true);
    }

    @Test
    void testRollBack() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        DBManager.setAutoCommit(connection, dbType, false);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 1);

        boolean isRollBack = DBManager.rollback(connection, dbType);
        Assertions.assertTrue(isRollBack);

        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 0);

        DBManager.setAutoCommit(connection, dbType, true);

        // more test cases
        DBManager.closeConnection(connection);
        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        insertData(connection, dbType);

        isRollBack = DBManager.rollback(connection, dbType);
        Assertions.assertFalse(isRollBack);

        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 1);

    }

    @Test
    void testGetTableData() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        insertData(connection, dbType);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        expectMap.put("age", 30);
        expectResult.add(expectMap);
        Assertions.assertEquals(result, expectResult);

        createView(connection, dbType);
        List<Map<String, Object>> result2= DBManager.getTableOrViewData(connection, dbType, "", "employee_view");
        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("last_name", "Doe");
        expectMap2.put("first_name", "John");
        expectMap2.put("email", "john.doe@example.com");
        expectResult2.add(expectMap2);
        Assertions.assertEquals(result2, expectResult2);

        // more test cases
        Assertions.assertEquals(expectResult, DBManager.getTableOrViewData(connection, dbType, null, "employees"));

        try {
            DBManager.getTableOrViewData(connection, dbType, "fakeSchema", "employee_view");
            Assertions.fail();
        } catch (SQLException e) {
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
        Connection connection = DBManager.createConnection(driver, url, username, password);
        createTable(connection, dbType);
        insertData(connection, dbType);
        insertData1(connection, dbType);
        insertData2(connection, dbType);


        List<String> columnNames = new ArrayList<>();
        columnNames.add("first_name");
        columnNames.add("last_name");
        columnNames.add("email");
        Map<String, Object> result = DBManager.getDataWithPage(connection, dbType, "", "employees", columnNames, 1, 5);
        Assertions.assertEquals(result.get("totalPages"), 3);

        List<Map<String, Object>> resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 5);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        Assertions.assertEquals(resultList.get(0), expectMap);

        result = DBManager.getDataWithPage(connection, dbType, "", "employees", new ArrayList<>(), 5, 1);
        Assertions.assertEquals(result.get("totalPages"), 12);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("id", 5);
        expectMap1.put("first_name", "Charlie");
        expectMap1.put("last_name", "Williams");
        expectMap1.put("email", "charlie.williams@example.com");
        expectMap1.put("age", 28);
        Assertions.assertEquals(resultList.get(0), expectMap1);

        result = DBManager.getDataWithPage(connection, dbType, "", "employees", new ArrayList<>(), 1, 12);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("id", 12);
        expectMap2.put("first_name", "Jane");
        expectMap2.put("last_name", "Jackson");
        expectMap2.put("email", "jane.jackson@example.com");
        expectMap2.put("age", 36);
        Assertions.assertEquals(resultList.get(11), expectMap2);

        columnNames = new ArrayList<>();
        columnNames.add("id");
        columnNames.add("first_name");
        columnNames.add("last_name");
        columnNames.add("email");
        columnNames.add("age");
        result = DBManager.getDataWithPage(connection, dbType, "", "employees", columnNames, 1, 100);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.get(11), expectMap2);

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

        result = DBManager.getDataWithPage(connection, dbType, "", "employees", null, 3, 10);
        Assertions.assertEquals(result.get("totalPages"), 2);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 0);
    }

    @Test
    void testMultiTypeData() throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.createConnection(driver, url, username, password);
        String sql = """
                CREATE TABLE MyTable
                  (
                      MyDecimal   REAL,          -- SQLite does not have a DECIMAL type, use REAL
                      MyDateTime TEXT,          -- Store as TEXT in format "YYYY-MM-DD HH:MM:SS.SSS"
                      MyBlob      BLOB           -- Store as BLOB
                  );
                """;
        String insertData = """
                INSERT INTO MyTable
                (
                  MyDecimal,
                  MyDateTime,
                  MyBlob
                )
                VALUES
                (
                  1234.56,                  -- Numeric value
                  '2023-05-31 12:34:56.789',-- Date and Time
                  X'44424D61736B6572'             -- BLOB data, inserting the Hex representation of "DBMasker"
                );
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(insertData);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "MyTable");
        Assertions.assertEquals(result.size(), 1);
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("MyDecimal", Double.parseDouble("1234.56"));

        expectMap.put("MyDateTime", "2023-05-31 12:34:56.789");

        expectMap.put("MyBlob", "DBMasker".getBytes());

        Assertions.assertEquals(expectMap.get("MyDecimal"), result.get(0).get("MyDecimal"));
        Assertions.assertEquals(expectMap.get("MyDateTime"), result.get(0).get("MyDateTime"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("MyBlob"), (byte[]) result.get(0).get("MyBlob"));
    }
}
