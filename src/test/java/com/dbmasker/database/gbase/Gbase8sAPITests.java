package com.dbmasker.database.gbase;

import com.dbmasker.api.DBManager;
import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.data.TableIndex;
import com.dbmasker.data.TableMetaData;
import com.dbmasker.database.DbType;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

class Gbase8sAPITests {
    private String driver = "com.gbasedbt.jdbc.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.GBASE8S.getDbName();
    private String version = "v8";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE IF NOT EXISTS employees (
                      id SERIAL PRIMARY KEY,
                      first_name VARCHAR(255) NOT NULL,
                      last_name VARCHAR(255) NOT NULL,
                      email VARCHAR(255) NOT NULL,
                      age INT
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
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('Alice', 'Smith', 'alice.smith@example.com', 30);
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('Bob', 'Johnson', 'bob.johnson@example.com', 35);
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('Charlie', 'Williams', 'charlie.williams@example.com', 28);
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('David', 'Brown', 'david.brown@example.com', 42);
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('Eva', 'Jones', 'eva.jones@example.com', 26);
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('Frank', 'Garcia', 'frank.garcia@example.com', 33);
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('Grace', 'Martinez', 'grace.martinez@example.com', 29);
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('Hannah', 'Anderson', 'hannah.anderson@example.com', 31);
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('Ivan', 'Thomas', 'ivan.thomas@example.com', 27);
               INSERT INTO employees (first_name, last_name, email, age) VALUES ('Jane', 'Jackson', 'jane.jackson@example.com', 36);
               """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createFunc(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE FUNCTION add_numbers(a INT, b INT)
                    RETURNING INT AS add_numbers;
                    RETURN(a+b);
                    END FUNCTION;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createDatabase(Connection connection, String dbType) throws SQLException {
        String sql = "create database db_gbase_test;";
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createSchema(Connection connection, String dbType) throws SQLException {
        String sql = """
                 CREATE SCHEMA AUTHORIZATION my_schema;
                 """;
        String sql2 = """
                 CREATE TABLE IF NOT EXISTS employees (
                     id SERIAL PRIMARY KEY,
                     first_name VARCHAR(255) NOT NULL,
                     last_name varchar(255) NOT NULL,
                     email varchar(255) NOT NULL,
                     age int
                 );
                 """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        DBManager.executeUpdateSQL(connection, dbType, sql2);
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
        String sql1 = """
                DROP TABLE IF EXISTS employees;
                """;
        String sql2 = """
                DROP FUNCTION IF EXISTS add_numbers;
                """;
        String sql3 = """
                DROP TABLE IF EXISTS my_table;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql1);
        DBManager.executeUpdateSQL(connection, dbType, sql2);
        DBManager.executeUpdateSQL(connection, dbType, sql3);
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
        Assertions.assertNotNull(DBManager.createConnection(driver, url, username, password));
    }

    @Test
    void testCloseConn() throws SQLException, ClassNotFoundException {
        Connection tmpConnection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertNotNull(tmpConnection);

        Assertions.assertTrue(DBManager.closeConnection(tmpConnection));

        // more test cases
        Assertions.assertTrue(tmpConnection.isClosed());
        Assertions.assertTrue(DBManager.closeConnection(tmpConnection));

        Assertions.assertFalse(DBManager.closeConnection(null));
    }

    @Test
    void testGetSchema() throws SQLException {
        // not support
    }

    @Test
    void testGetTables() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<String> tables = DBManager.getTables(connection, dbType, "");
        List<String> expectTables = new ArrayList<>();
        expectTables.add("employees");

        Assertions.assertEquals(expectTables, tables);

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

        Assertions.assertTrue(DBManager.getTables(connection, dbType, null).contains("employees"));
        Assertions.assertTrue(DBManager.getTables(connection, dbType, "fakeSchema").contains("employees"));

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getTables(connection, dbType, ""));
    }

    @Test
    void testGetViews() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);
        createView(connection, dbType);

        List<String> resultViews = DBManager.getViews(connection, dbType, "");
        List<String> expectViews = new ArrayList<>();
        expectViews.add("employee_view");

        Assertions.assertTrue(resultViews.contains("employee_view"));

        // more test cases
        Assertions.assertTrue(DBManager.getViews(connection, dbType, null).contains("employee_view"));
        Assertions.assertTrue(DBManager.getViews(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertFalse(DBManager.getViews(connection, dbType, "").contains("employee_view"));
    }

    @Test
    void testGetMetaData() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<TableMetaData> metaDatas = DBManager.getMetaData(connection, dbType, "");
        Assertions.assertEquals(metaDatas.size(), 5);

        // more test cases
        Assertions.assertEquals(5, DBManager.getMetaData(connection, dbType, null).size());
        Assertions.assertEquals(0, DBManager.getMetaData(connection, dbType, "fakeSchema").size());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getMetaData(connection, dbType, ""));
    }

    @Test
    void testGetTableAttribute() throws SQLException {
        createTable(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "", "employees");
        Assertions.assertEquals(tableAttributes.size(), 5);

        // more test cases
        Assertions.assertEquals(5, DBManager.getTableAttribute(connection, dbType, null, "employees").size());
        Assertions.assertEquals(0, DBManager.getTableAttribute(connection, dbType, "fakeSchema", "employees").size());

        try {
            DBManager.getTableAttribute(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertTrue(DBManager.getTableAttribute(connection, dbType, null, "fakeTable").isEmpty());

        Assertions.assertTrue(DBManager.getTableAttribute(connection, dbType, null, "").size() >= 5);
    }

    @Test
    void testGetPrimaryKeys() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<String> tablePrimaryKeys = DBManager.getPrimaryKeys(connection, dbType, "", "employees");
        Assertions.assertEquals(1, tablePrimaryKeys.size());

        List<String> expectPrimaryKeys = new ArrayList<>();
        expectPrimaryKeys.add("id");

        Assertions.assertEquals(expectPrimaryKeys, tablePrimaryKeys);

        // more test cases
        Assertions.assertEquals(1, DBManager.getPrimaryKeys(connection, dbType, null, "employees").size());
        Assertions.assertEquals(0, DBManager.getPrimaryKeys(connection, dbType, "fakeSchema", "employees").size());

        try {
            DBManager.getPrimaryKeys(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getPrimaryKeys(connection, dbType, "", "fakeTable").size());
    }

    @Test
    void testGetUniqueKey() throws SQLException {
        createTable(connection, dbType);

        Map<String, Set<String>> tableUniqueKey = DBManager.getUniqueKeys(connection, dbType, "", "employees");
        Assertions.assertEquals(tableUniqueKey.size(), 1);

        // more test cases
        Assertions.assertEquals(1, DBManager.getUniqueKeys(connection, dbType, null, "employees").size());
        Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, "fakeSchema", "employees").size());

        try {
            DBManager.getUniqueKeys(connection, dbType, null, null).size();
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, null, "fakeTable").size());
    }

    @Test
    void testGetIndex() throws SQLException {
        createTable(connection, dbType);

        List<TableIndex> tableIndex = DBManager.getIndex(connection, dbType, "", "employees");
        TableIndex tableIndex2 = new TableIndex("xxx", "id", true);
        List<TableIndex> expectIndex = new ArrayList<>();
        expectIndex.add(tableIndex2);
        Assertions.assertEquals(tableIndex.size(), expectIndex.size());
        Assertions.assertEquals(tableIndex2.getColumnName(), expectIndex.get(0).getColumnName());

        // more test cases
        Assertions.assertEquals(expectIndex.size(), DBManager.getIndex(connection, dbType, null, "employees").size());
        Assertions.assertEquals(0, DBManager.getIndex(connection, dbType, "fakeSchema", "employees").size());

        try {
            Assertions.assertEquals(0, DBManager.getIndex(connection, dbType, null, null).size());
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getIndex(connection, dbType, null, "fakeTable").size());
    }

    @Test
    void testExecuteUpdateSQL() throws SQLException {
        createTable(connection, dbType);
        String insert = """
                INSERT INTO employees (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        int rowCount = DBManager.executeUpdateSQL(connection, dbType, insert);
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

        try {
            DBManager.executeUpdateSQL(connection, dbType, "SELECT id, first_name, last_name, email, age FROM employees;");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testExecuteUpdateSQLBatch() throws SQLException {
        List<String> sqlList = new ArrayList<>();
        String createTableSql = """
                CREATE TABLE employees (
                     id SERIAL PRIMARY KEY,
                     first_name VARCHAR(255) NOT NULL,
                     last_name varchar(255) NOT NULL,
                     email varchar(255) NOT NULL,
                     age int
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
        sqlList.add(createTableSql);
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
            Assertions.assertEquals(2, DBManager.getTableOrViewData(connection, dbType, "", "employees").size());
        }
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
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
    void testExecuteQuerySQLBatch() throws SQLException {
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

        try {
            DBManager.executeSQL(connection, dbType, sql);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        sql = """
                UPDATE employees SET age = 32 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
        try {
            DBManager.executeSQL(connection, dbType, sql);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeSQL(connection, dbType, "fakeSQL");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testExecuteSQLBatch() throws SQLException {
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
    void testExecuteSQLScript() throws SQLException {
        createSchema(connection, dbType);
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
            expectMap.put("age", 33);
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
    void testCommit() {
        // not support
    }

    @Test
    void testRollBack() {
        // not support
    }

    @Test
    void testGetTableData() throws SQLException {
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
    void testGetFunc() throws SQLException {
        createTable(connection, dbType);
        createFunc(connection, dbType);

        List<DatabaseFunction> result = DBManager.getFuncs(connection, dbType, "");
        Assertions.assertEquals(1, result.size());

        DatabaseFunction function = new DatabaseFunction("", "add_numbers");
        List<DatabaseFunction> expect = new ArrayList<>();
        expect.add(function);
        Assertions.assertEquals(expect, result);

        // more test cases
        Assertions.assertEquals(expect.size(), DBManager.getFuncs(connection, dbType, null).size());
        Assertions.assertEquals(expect.size(), DBManager.getFuncs(connection, dbType, "fakeSchema").size());
    }

    @Test
    void testExecuteFunction() throws SQLException {
        createTable(connection, dbType);
        createFunc(connection, dbType);

        List<Map<String, Object>> result = DBManager.executeFunction(connection, dbType, "", "add_numbers", 10, 20);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("add_numbers", 30);
        List<Map<String, Object>> expect = new ArrayList<>();
        expect.add(expectMap);

        Assertions.assertEquals(expect, result);

        // more test cases
        try {
            DBManager.executeFunction(connection, dbType, "", "fakeFunction", 10, 20);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeFunction(connection, dbType, "", "add_numbers", 10);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeFunction(connection, dbType, "", "add_numbers", 10, "abc");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        Assertions.assertEquals(expect, DBManager.executeFunction(connection, dbType, null, "add_numbers", 10, 20));
    }

    @Test
    void testMultiTypeData() throws SQLException, IOException {
        String sql = """
                CREATE TABLE my_table
                (
                  my_decimal   DECIMAL(10, 2),  -- DECIMAL type, total length is 10, decimal places are 2
                  my_date      DATE,            -- Date type
                  my_timestamp TIMESTAMP,       -- Timestamp type
                  my_boolean   BOOLEAN         -- Boolean type, actually a TINYINT type in GBase 8a
                );
                """;
        String insertData = """
                INSERT INTO my_table
                (
                    my_decimal,
                    my_date,
                    my_timestamp,
                    my_boolean
                )
                VALUES
                (
                    1234.56,
                    '2023-05-31',
                    '2023-05-31 12:34:56',
                     't'
                );
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(insertData);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "my_table");
        Assertions.assertEquals(result.size(), 1);
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("my_decimal", new BigDecimal("1234.56"));

        Date date = Date.valueOf("2023-05-31");
        expectMap.put("my_date", date);

        expectMap.put("my_timestamp", Timestamp.valueOf("2023-05-31 12:34:56"));
        expectMap.put("my_boolean", true);

        Assertions.assertEquals(expectMap.get("my_decimal"), result.get(0).get("my_decimal"));
        Assertions.assertEquals(expectMap.get("my_date").toString(), result.get(0).get("my_date").toString());
        Assertions.assertEquals(expectMap.get("my_timestamp"), result.get(0).get("my_timestamp"));
        Assertions.assertEquals(expectMap.get("my_boolean"), result.get(0).get("my_boolean"));
    }
}
