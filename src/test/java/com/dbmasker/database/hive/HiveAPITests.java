package com.dbmasker.database.hive;

import com.dbmasker.api.DBManager;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.data.TableIndex;
import com.dbmasker.data.TableMetaData;
import com.dbmasker.database.DbType;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import org.apache.hive.service.cli.HiveSQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

class HiveAPITests {

    private String driver = "org.apache.hive.jdbc.HiveDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.HIVE.getDbName();
    private String version = "v2";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                  CREATE TABLE employees (
                      id INT,
                      first_name STRING,
                      last_name STRING,
                      email STRING,
                      age INT
                  ) ROW FORMAT DELIMITED
                  FIELDS TERMINATED BY ','
                  STORED AS TEXTFILE
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createMyTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE my_table (
                    my_decimal   DECIMAL(10, 2),     -- DECIMAL type, total length is 10, decimal places are 2
                    my_date      DATE,               -- Date type
                    my_timestamp TIMESTAMP,          -- Timestamp type
                    my_boolean   BOOLEAN,            -- Boolean type
                    my_blob      BINARY              -- Hive doesn't have a specific BLOB type, BINARY can be used instead
                ) ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertMyTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_table
                (
                  my_decimal,
                  my_date,
                  my_timestamp,
                  my_boolean,
                  my_blob
                )
                VALUES
                (
                  1234.56,                     -- Decimal value
                  '2023-05-31',                -- Date
                  '2023-05-31 12:34:56',       -- Timestamp
                  TRUE,                        -- Boolean value
                  'DBMasker'                   -- Binary data, inserting the Hex representation of "DBMasker"
                )
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }


    public void createView(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE VIEW employee_view AS
                SELECT first_name, last_name, email
                FROM employees
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO employees VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30)
                """;
        int rows = DBManager.executeUpdateSQL(connection, dbType, sql);
        Assertions.assertEquals(0, rows);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO employees2 VALUES (2, 'Jane', 'Smith', 'jane.smith@example.com', 28)
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData2(Connection connection, String dbType) throws SQLException {
        String sql = """
               INSERT INTO employees2 (id, first_name, last_name, email, age) VALUES
                   (3, 'Alice', 'Smith', 'alice.smith@example.com', 30),
                   (4, 'Bob', 'Johnson', 'bob.johnson@example.com', 35),
                   (5, 'Charlie', 'Williams', 'charlie.williams@example.com', 28),
                   (6, 'David', 'Brown', 'david.brown@example.com', 42),
                   (7, 'Eva', 'Jones', 'eva.jones@example.com', 26),
                   (8, 'Frank', 'Garcia', 'frank.garcia@example.com', 33),
                   (9, 'Grace', 'Martinez', 'grace.martinez@example.com', 29),
                   (10, 'Hannah', 'Anderson', 'hannah.anderson@example.com', 31),
                   (11, 'Ivan', 'Thomas', 'ivan.thomas@example.com', 27),
                   (12, 'Jane', 'Jackson', 'jane.jackson@example.com', 36)
               """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createDatabase(Connection connection, String dbType) throws SQLException {
        String sql = "CREATE DATABASE db_hive_test";
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    private void doInit() throws SQLException {
        createTable(connection, dbType);
        createView(connection, dbType);
        insertData(connection, dbType);
        insertData1(connection, dbType);
        insertData2(connection, dbType);
    }

    private void doInit2() throws SQLException {
        createMyTable(connection, dbType);
        insertMyTable(connection, dbType);
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

        List<String> tables = DBManager.getTables(connection, dbType, "");
        if (!tables.contains("employees2")) {
            doInit();
        }

        if (!tables.contains("my_table")) {
            doInit2();
        }
    }

    @AfterEach
    public void tearDown() throws SQLException {
        String sql = "DROP TABLE IF EXISTS employees";
        DBManager.executeUpdateSQL(connection, dbType, sql);

        String sql1 = "DROP VIEW IF EXISTS employee_view";
        DBManager.executeUpdateSQL(connection, dbType, sql1);

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
    void testGetSchema()  {
        //not support
    }

    @Test
    void testGetTables() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<String> tables = DBManager.getTables(connection, dbType, "");

        Assertions.assertTrue(tables.contains("employees"));

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
        Assertions.assertFalse(DBManager.getTables(connection, dbType, "").contains("employees"));
    }

    @Test
    void testGetViews() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);
        createView(connection, dbType);

        List<String> resultViews = DBManager.getViews(connection, dbType, "");
        List<String> expectViews = new ArrayList<>();
        expectViews.add("employee_view2");
        expectViews.add("employee_view");

        Assertions.assertEquals(expectViews, resultViews);

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
        Assertions.assertTrue(metaDatas.size() > 5);

        // more test cases
        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, null).size() > 5);
        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, "").size() >= 0);
    }

    @Test
    void testGetTableAttribute() throws SQLException {
        createTable(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "", "employees");
        Assertions.assertEquals(tableAttributes.size(), 5);

        // more test cases
        try {
            DBManager.getTableAttribute(connection, dbType, null, "employees");
            Assertions.fail();
        } catch (SQLException e) {
            // pass
        }
        Assertions.assertEquals(0, DBManager.getTableAttribute(connection, dbType, "fakeSchema", "employees").size());

        try {
            DBManager.getTableAttribute(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        try {
            DBManager.getTableAttribute(connection, dbType, null, "fakeTable");
            Assertions.fail();
        } catch (SQLException e) {
            // pass
        }

        try {
            DBManager.getTableAttribute(connection, dbType, null, "");
            Assertions.fail();
        } catch (SQLException e) {
            // pass
        }
    }

    @Test
    void testGetUniqueKey() throws SQLException {
        createTable(connection, dbType);

        Map<String, Set<String>> tableUniqueKey = DBManager.getUniqueKeys(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(tableUniqueKey.size(), 0);

        // more test cases
        Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, null, "employees").size());
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
        Assertions.assertEquals(tableIndex.size(), 0);

        // more test cases
        Assertions.assertEquals(0, DBManager.getIndex(connection, dbType, null, "employees").size());
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
            DBManager.executeUpdateSQL(connection, dbType, "SELECT id, first_name, last_name, email, age FROM my_schema.employees;");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testExecuteUpdateSQLBatch() throws SQLException {
        List<String> sqlList = new ArrayList<>();
        String createTable = """
                CREATE TABLE employees (
                      id INT,
                      first_name STRING,
                      last_name STRING,
                      email STRING,
                      age INT
                  ) ROW FORMAT DELIMITED
                  FIELDS TERMINATED BY ','
                """;
        String createView = """
                CREATE VIEW employee_view AS
                SELECT first_name, last_name, email
                FROM employees
                """;
        sqlList.add(createTable);
        sqlList.add(createView);

        int rowCount = DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(rowCount, 0);

        // more test cases
        try {
            DBManager.executeUpdateSQLBatch(connection, dbType, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_SQL_LIST_ERROR));
        }

        Assertions.assertEquals(0, DBManager.executeUpdateSQLBatch(connection, dbType, new ArrayList<>()));

        String insert2 = """
                UPDATE employees SET age = 31 WHERE id = 1
                """;
        String fakeSQL = "fakeSQL";
        ArrayList<String> sqlList2 = new ArrayList<>();
        sqlList2.add(insert2);
        sqlList2.add(fakeSQL);
        try {
            DBManager.executeUpdateSQLBatch(connection, dbType, sqlList2);
            Assertions.fail();
        } catch (SQLException e) {
            List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
            Assertions.assertEquals(0, result.size());
        }
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        // do in testExecuteQuerySQLAndBatch
    }

    @Test
    void testExecuteQuerySQLAndBatch() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        // test cases for getTableOrViewData
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "employees");
        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("employees.last_name", "Doe");
        expectMap.put("employees.id", 1);
        expectMap.put("employees.first_name", "John");
        expectMap.put("employees.email", "john.doe@example.com");
        expectMap.put("employees.age", 30);
        expectResult.add(expectMap);
        Assertions.assertEquals(result, expectResult);

        createView(connection, dbType);
        List<Map<String, Object>> result2= DBManager.getTableOrViewData(connection, dbType, "", "employee_view");
        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("employee_view.last_name", "Doe");
        expectMap2.put("employee_view.first_name", "John");
        expectMap2.put("employee_view.email", "john.doe@example.com");
        expectResult2.add(expectMap2);
        Assertions.assertEquals(result2, expectResult2);

        // more test cases for getTableOrViewData
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

        // test cases for executeQuerySQL
        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees
                """;
        result = DBManager.executeQuerySQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);

        expectMap = new HashMap<>();
        expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        expectMap.put("age", 30);
        expectResult.add(expectMap);

        Assertions.assertEquals(result, expectResult);

        // test cases for executeSQL
        result = DBManager.executeSQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);

        Assertions.assertEquals(result, expectResult);

        String updateSql = """
                UPDATE employees SET age = 31 WHERE id = 1
                """;
        try {
            DBManager.executeSQL(connection, dbType, updateSql);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        // more test cases for executeSQL
        String sql1 = """
                SELECT id, first_name, last_name, email, age FROM employees;
                UPDATE employees SET age = 32 WHERE id = 1
                """;

        try {
            DBManager.executeSQL(connection, dbType, sql1);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        // test cases for executeSQLScript
        String sqlScript = """
                SELECT id, first_name, last_name, email, age FROM employees;
                SELECT id, first_name, last_name, email, age FROM employees;
                """;

        List<List<Map<String, Object>>> resultScript = DBManager.executeSQLScript(connection, dbType, sqlScript);
        Assertions.assertEquals(resultScript.size(), 2);

        Assertions.assertEquals(resultScript.get(0), expectResult);
        Assertions.assertEquals(resultScript.get(1), expectResult);


        sql1 = """
                UPDATE employees SET age = 32 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM employees
                """;
        try {
            DBManager.executeSQL(connection, dbType, sql1);
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

        // test cases for executeQuerySQLBatch
        String sql2 = """
                SELECT first_name, last_name, email FROM employees
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(sql2);

        List<List<Map<String, Object>>> resultBatch = DBManager.executeQuerySQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(resultBatch.size(), 2);

        Map<String, Object> expectMap1 = new HashMap<>();
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectMap1.put("last_name", "Doe");
        expectMap1.put("id", 1);
        expectMap1.put("first_name", "John");
        expectMap1.put("email", "john.doe@example.com");
        expectMap1.put("age", 30);
        expectResult1.add(expectMap1);

        expectMap2 = new HashMap<>();
        expectResult2 = new ArrayList<>();
        expectMap2.put("last_name", "Doe");
        expectMap2.put("first_name", "John");
        expectMap2.put("email", "john.doe@example.com");
        expectResult2.add(expectMap2);

        List<List<Map<String, Object>>> expectResults = new ArrayList<>();
        expectResults.add(expectResult1);
        expectResults.add(expectResult2);

        Assertions.assertEquals(expectResults, resultBatch);

        // more test cases for executeQuerySQL
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

        String update = """
                UPDATE employees SET age = 31 WHERE id = 1
                """;
        try {
            DBManager.executeQuerySQL(connection, dbType, update);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        // more test cases for executeQuerySQLBatch
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
        updateSql = """
                UPDATE employees SET age = 31 WHERE id = 1
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
    void testExecuteSQLScript() throws SQLException {
        String sql = """
                CREATE TABLE employees (
                  id INT,
                  first_name STRING,
                  last_name STRING,
                  email STRING,
                  age INT
                ) ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE;
                CREATE VIEW employee_view AS
                SELECT first_name, last_name, email
                FROM employees;
                SELECT id, first_name, last_name, email, age FROM employees;
                  """;
        List<List<Map<String, Object>>> result = DBManager.executeSQLScript(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 3);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("rows", 0);
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectResult.add(expectMap);

        Assertions.assertEquals(expectResult, result.get(0));
        Assertions.assertEquals(expectResult, result.get(1));
        Assertions.assertTrue(result.get(2).isEmpty());

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
        // do in testExecuteQuerySQLAndBatch
    }

    @Test
    void testGetDataWithPage() throws SQLException, ClassNotFoundException {
        List<String> columnNames = new ArrayList<>();
        columnNames.add("first_name");
        columnNames.add("last_name");
        columnNames.add("email");
        Map<String, Object> result = DBManager.getDataWithPage(connection, dbType, "", "employees2", columnNames, 1, 5);
        Assertions.assertEquals(result.get("totalPages"), 3);

        List<Map<String, Object>> resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 5);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        Assertions.assertEquals(resultList.get(0), expectMap);

        result = DBManager.getDataWithPage(connection, dbType, "", "employees2", new ArrayList<>(), 5, 1);
        Assertions.assertEquals(result.get("totalPages"), 12);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("employees2.id", 5);
        expectMap1.put("employees2.first_name", "Charlie");
        expectMap1.put("employees2.last_name", "Williams");
        expectMap1.put("employees2.email", "charlie.williams@example.com");
        expectMap1.put("employees2.age", 28);
        Assertions.assertEquals(resultList.get(0), expectMap1);

        result = DBManager.getDataWithPage(connection, dbType, "", "employees2", new ArrayList<>(), 1, 12);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("employees2.id", 12);
        expectMap2.put("employees2.first_name", "Jane");
        expectMap2.put("employees2.last_name", "Jackson");
        expectMap2.put("employees2.email", "jane.jackson@example.com");
        expectMap2.put("employees2.age", 36);
        Assertions.assertEquals(resultList.get(11), expectMap2);

        columnNames = new ArrayList<>();
        columnNames.add("id");
        columnNames.add("first_name");
        columnNames.add("last_name");
        columnNames.add("email");
        columnNames.add("age");
        result = DBManager.getDataWithPage(connection, dbType, "", "employees2", null, 1, 100);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.get(11), expectMap2);

        result = DBManager.getDataWithPage(connection, dbType, "", "employees2", null, -1, 5);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        result = DBManager.getDataWithPage(connection, dbType, "", "employees2", null, 1, 0);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        // more test cases
        columnNames = new ArrayList<>();
        columnNames.add("first_name");
        columnNames.add("fake_column");
        try {
            DBManager.getDataWithPage(connection, dbType, "", "employees2", columnNames, 1, 5);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.getDataWithPage(connection, dbType, "fakeSchema", "employees2", null, 1, 5);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        result = DBManager.getDataWithPage(connection, dbType, "", "employees2", null, 3, 10);
        Assertions.assertEquals(result.get("totalPages"), 2);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 0);
    }

    @Test
    void testGetFunc() {
        // not support
    }

    @Test
    void testExecuteFunction(){
        // not support
    }

    @Test
    void testMultiTypeData() throws SQLException {
        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "", "my_table");
        Assertions.assertEquals(result.size(), 1);
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("my_decimal", new BigDecimal("1234.56"));

        LocalDate localDate = LocalDate.parse("2023-05-31");
        ZoneId defaultZoneId = ZoneId.systemDefault();
        Instant instant = localDate.atStartOfDay(defaultZoneId).toInstant();
        Date date = Date.from(instant);
        expectMap.put("my_date", date);

        expectMap.put("my_timestamp", Timestamp.valueOf("2023-05-31 12:34:56"));
        expectMap.put("my_boolean", true);

        expectMap.put("my_blob", "DBMasker".getBytes());

        Assertions.assertEquals(expectMap.get("my_decimal"), result.get(0).get("my_table.my_decimal"));
        Assertions.assertEquals(expectMap.get("my_date"), result.get(0).get("my_table.my_date"));
        Assertions.assertEquals(expectMap.get("my_timestamp"), result.get(0).get("my_table.my_timestamp"));
        Assertions.assertEquals(expectMap.get("my_boolean"), result.get(0).get("my_table.my_boolean"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("my_blob"), (byte[]) result.get(0).get("my_table.my_blob"));
    }

}
