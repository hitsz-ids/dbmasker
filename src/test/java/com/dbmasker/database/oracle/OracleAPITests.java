package com.dbmasker.database.oracle;

import com.dbmasker.api.DBManager;
import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.data.TableIndex;
import com.dbmasker.data.TableMetaData;
import com.dbmasker.database.DbType;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import oracle.net.nt.TcpMultiplexer;
import oracle.sql.BLOB;
import oracle.sql.INTERVALDS;
import oracle.sql.TIMESTAMP;
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
import java.time.*;
import java.util.*;

class OracleAPITests {
    private String driver = "oracle.jdbc.driver.OracleDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.ORACLE.getDbName();
    private String version = "v11";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE employees (
                   id INT NOT NULL,
                   first_name VARCHAR2(255) NOT NULL,
                   last_name VARCHAR2(255) NOT NULL,
                   email VARCHAR2(255) NOT NULL UNIQUE,
                   age INT
                )
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createTable1 (Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE employees (
                   id INT NOT NULL,
                   first_name VARCHAR2(255) NOT NULL,
                   last_name VARCHAR2(255) NOT NULL,
                   email VARCHAR2(255) NOT NULL ,
                   age INT,
                   PRIMARY KEY (id)
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
                INSERT INTO employees (id, first_name, last_name, email, age)
                VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30)
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO employees VALUES (2, 'Jane', 'Smith', 'jane.smith@example.com', 28)
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData2(Connection connection, String dbType) throws SQLException {
        String sql = "INSERT ALL" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (3, 'Alice', 'Smith', 'alice.smith@example.com', 30)" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (4, 'Bob', 'Johnson', 'bob.johnson@example.com', 35)" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (5, 'Charlie', 'Williams', 'charlie.williams@example.com', 28)" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (6, 'David', 'Brown', 'david.brown@example.com', 42)" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (7, 'Eva', 'Jones', 'eva.jones@example.com', 26)" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (8, 'Frank', 'Garcia', 'frank.garcia@example.com', 33)" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (9, 'Grace', 'Martinez', 'grace.martinez@example.com', 29)" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (10, 'Hannah', 'Anderson', 'hannah.anderson@example.com', 31)" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (11, 'Ivan', 'Thomas', 'ivan.thomas@example.com', 27)" +
                " INTO employees (id, first_name, last_name, email, age) VALUES (12, 'Jane', 'Jackson', 'jane.jackson@example.com', 36)" +
                " SELECT * FROM dual";
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createFunc(Connection connection, String dbType) throws SQLException {
        String sql2 = """
                 CREATE FUNCTION add_numbers(a INT, b INT) RETURN INT
                 AS
                 BEGIN
                     RETURN a + b;
                 END;
                 """;
        DBManager.executeUpdateSQL(connection, dbType, sql2);
    }

    public void createIndex(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE INDEX employee_email_idx ON employees (email)
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/oracle.properties")) {
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
                BEGIN
                   EXECUTE IMMEDIATE 'DROP TABLE employees';
                EXCEPTION
                   WHEN OTHERS THEN
                      IF SQLCODE != -942 THEN
                         RAISE;
                      END IF;
                END;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);

        sql = """
                BEGIN
                   EXECUTE IMMEDIATE 'DROP TABLE MyTable';
                EXCEPTION
                   WHEN OTHERS THEN
                      IF SQLCODE != -942 THEN
                         RAISE;
                      END IF;
                END;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);

        String sql1 = """
                BEGIN
                   EXECUTE IMMEDIATE 'DROP VIEW employee_view';
                EXCEPTION
                   WHEN OTHERS THEN
                      IF SQLCODE != -942 THEN
                         RAISE;
                      END IF;
                END;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql1);

        String sql2 = """
                BEGIN
                   EXECUTE IMMEDIATE 'DROP FUNCTION add_numbers';
                EXCEPTION
                   WHEN OTHERS THEN
                      IF SQLCODE != -4043 THEN
                         RAISE;
                      END IF;
                END;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql2);

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
        Connection connection1 = DBManager.createConnection(driver, url, username, password);
        Assertions.assertNotNull(connection1);

        Assertions.assertTrue(DBManager.closeConnection(connection1));

        // more test cases
        Assertions.assertTrue(connection1.isClosed());
        Assertions.assertTrue(DBManager.closeConnection(connection1));

        Assertions.assertFalse(DBManager.closeConnection(null));
    }

    @Test
    void testGetSchema() throws SQLException {
        List<String> sqliteSchemas = DBManager.getSchema(connection, dbType);
        Assertions.assertTrue(sqliteSchemas.size() > 0);
        Assertions.assertTrue(sqliteSchemas.contains("M"));
    }

    @Test
    void testGetTables() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<String> tables = DBManager.getTables(connection, dbType, "M");
        List<String> expectTables = new ArrayList<>();
        expectTables.add("EMPLOYEES");

        Assertions.assertEquals(tables.contains("EMPLOYEES"), true);

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

        Assertions.assertTrue(DBManager.getTables(connection, dbType, null).contains("EMPLOYEES"));
        Assertions.assertTrue(DBManager.getTables(connection, dbType, "fakeSchema").isEmpty());
    }

    @Test
    void testGetViews() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);
        createView(connection, dbType);

        List<String> views = DBManager.getViews(connection, dbType, "M");

        Assertions.assertTrue(views.contains("EMPLOYEE_VIEW"));

        // more test cases
        Assertions.assertTrue(DBManager.getViews(connection, dbType, null).contains("EMPLOYEE_VIEW"));
        Assertions.assertTrue(DBManager.getViews(connection, dbType, "fakeSchema").isEmpty());

        Assertions.assertEquals(new ArrayList<>(), DBManager.getViews(connection, dbType, "my_schema"));
    }

    @Test
    void testGetMetaData() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<TableMetaData> metaDatas = DBManager.getMetaData(connection, dbType, "SYNC");
        Assertions.assertTrue(metaDatas.size() > 0);

        // clean cursors
        DBManager.closeConnection(connection);
        connection = DBManager.createConnection(driver, url, username, password);

        // more test cases
//        Assertions.assertTrue( DBManager.getMetaData(connection, dbType, null).size() > 5);
        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, "fakeSchema").isEmpty());

        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, "my_schema").isEmpty());
    }

    @Test
    void testGetTableAttribute() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(tableAttributes.size(), 5);

        // more test cases
        Assertions.assertEquals(5, DBManager.getTableAttribute(connection, dbType, null, "EMPLOYEES").size());
        Assertions.assertTrue(DBManager.getTableAttribute(connection, dbType, "fakeSchema", "EMPLOYEES").isEmpty());

        try {
            DBManager.getTableAttribute(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getTableAttribute(connection, dbType, null, "fakeTable").size());

        Assertions.assertTrue(DBManager.getTableAttribute(connection, dbType, null, "").isEmpty());
    }

    @Test
    void testGetPrimaryKeys() throws SQLException, ClassNotFoundException {
        createTable1(connection, dbType);

        List<String> tablePrimaryKeys = DBManager.getPrimaryKeys(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(1, tablePrimaryKeys.size());

        List<String> expectPrimaryKeys = new ArrayList<>();
        expectPrimaryKeys.add("ID");

        Assertions.assertEquals(expectPrimaryKeys, tablePrimaryKeys);

        // more test cases
        Assertions.assertEquals(1, DBManager.getPrimaryKeys(connection, dbType, null, "EMPLOYEES").size());
        Assertions.assertEquals(0, DBManager.getPrimaryKeys(connection, dbType, "fakeSchema", "EMPLOYEES").size());

        try {
            DBManager.getPrimaryKeys(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getPrimaryKeys(connection, dbType, "my_schema", "fakeTable").size());
    }

    @Test
    void testGetUniqueKey() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        Map<String, Set<String>> tableUniqueKey = DBManager.getUniqueKeys(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals(tableUniqueKey.size(), 1);

        // more test cases
        Assertions.assertEquals(1, DBManager.getUniqueKeys(connection, dbType, null, "EMPLOYEES").size());

        try {
            DBManager.getUniqueKeys(connection, dbType, "fakeSchema", "EMPLOYEES");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, null, null).size());
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        try {
            DBManager.getUniqueKeys(connection, dbType, null, "fakeTable");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testGetIndex() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<TableIndex> tableIndex = DBManager.getIndex(connection, dbType, "M", "EMPLOYEES");
        Assertions.assertEquals("EMAIL", tableIndex.get(0).getColumnName());

        // more test cases
        Assertions.assertEquals("EMAIL", DBManager.getIndex(connection, dbType, null, "EMPLOYEES").get(0).getColumnName());
        Assertions.assertTrue(DBManager.getIndex(connection, dbType, "fakeSchema", "employees").isEmpty());

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
                INSERT INTO employees (id, first_name, last_name, email, age)
                VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30)
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
            DBManager.executeUpdateSQL(connection, dbType, "SELECT id, first_name, last_name, email, age FROM my_schema.employees;");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testExecuteUpdateSQLBatch() throws SQLException, ClassNotFoundException {
        List<String> sqlList = new ArrayList<>();
        String createTable = """
                CREATE TABLE employees (
                   id NUMBER(10) NOT NULL,
                   first_name VARCHAR2(255) NOT NULL,
                   last_name VARCHAR2(255) NOT NULL,
                   email VARCHAR2(255) NOT NULL UNIQUE,
                   age NUMBER(3)
                )
                """;
        String createView = """
                CREATE VIEW employee_view AS
                SELECT first_name, last_name, email
                FROM employees
                """;
        String insert = """
                INSERT INTO employees (id, first_name, last_name, email, age)
                VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30)
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
                INSERT INTO employees VALUES (2, 'Jane', 'Smith', 'jane.smith@example.com', 28)
                """;
        String fakeSQL = "fakeSQL";
        ArrayList<String> sqlList2 = new ArrayList<>();
        sqlList2.add(insert2);
        sqlList2.add(fakeSQL);
        try {
            DBManager.executeUpdateSQLBatch(connection, dbType, sqlList2);
            Assertions.fail();
        } catch (SQLException e) {
            Assertions.assertEquals(1, DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES").size());
        }
    }

    @Test
    void testExecuteQuerySQL() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees
                """;
        List<Map<String, Object>> result = DBManager.executeQuerySQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", new BigDecimal(1));
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", new BigDecimal(30));
        expectResult.add(expectMap);

        Assertions.assertEquals(result.get(0).get("ID"), expectResult.get(0).get("ID"));
        Assertions.assertEquals(result.get(0).get("FIRST_NAME"), expectResult.get(0).get("FIRST_NAME"));
        Assertions.assertEquals(result.get(0).get("LAST_NAME"), expectResult.get(0).get("LAST_NAME"));
        Assertions.assertEquals(result.get(0).get("EMAIL"), expectResult.get(0).get("EMAIL"));
        Assertions.assertEquals(result.get(0).get("AGE"), expectResult.get(0).get("AGE"));

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
                INSERT INTO employees VALUES (2, 'Jane', 'Smith', 'jane.smith@example.com', 28)
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
        createTable(connection, dbType);
        insertData(connection, dbType);
        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees
                """;
        String sql2 = """
                SELECT first_name, last_name, email FROM employees
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(sql2);

        List<List<Map<String, Object>>> result = DBManager.executeQuerySQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(result.size(), 2);

        Map<String, Object> expectMap1 = new HashMap<>();
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectMap1.put("LAST_NAME", "Doe");
        expectMap1.put("ID", new BigDecimal(1));
        expectMap1.put("FIRST_NAME", "John");
        expectMap1.put("EMAIL", "john.doe@example.com");
        expectMap1.put("AGE", new BigDecimal(30));
        expectResult1.add(expectMap1);

        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("LAST_NAME", "Doe");
        expectMap2.put("FIRST_NAME", "John");
        expectMap2.put("EMAIL", "john.doe@example.com");
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
                UPDATE my_schema.employees SET age = 31 WHERE id = 1;
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
    void testExecuteSQL() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees
                """;
        List<Map<String, Object>> result = DBManager.executeSQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", new BigDecimal(1));
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", new BigDecimal(30));
        expectResult.add(expectMap);

        Assertions.assertEquals(result, expectResult);

        String updateSql = """
                UPDATE employees SET age = 31 WHERE id = 1
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
                SELECT id, first_name, last_name, email, age FROM employees
                UPDATE employees SET age = 32 WHERE id = 1
                """;

        try {
            DBManager.executeSQL(connection, dbType, sql);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        sql = """
                UPDATE employees SET age = 32 WHERE id = 1
                SELECT id, first_name, last_name, email, age FROM employees
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
                SELECT id, first_name, last_name, email, age FROM employees
                """;
        String sql2 = """
                UPDATE employees SET age = 31 WHERE id = 1
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql1);
        sqlList.add(sql2);

        List<List<Map<String, Object>>> result = DBManager.executeSQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(result.size(), 2);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", new BigDecimal(1));
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", new BigDecimal(30));
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
                UPDATE employees SET age = 32 WHERE id = 1
                """;
        sqlList = new ArrayList<>();
        sqlList.add(sql2);
        sqlList.add(sql1);

        result = DBManager.executeSQLBatch(connection, dbType, sqlList);
        expectResultList = new ArrayList<>();
        expectResultList.add(expectResult1);
        expectMap.put("AGE", new BigDecimal(32));
        expectResultList.add(expectResult);

        Assertions.assertEquals(result, expectResultList);
    }

    @Test
    void testExecuteSQLScript() throws SQLException {
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
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", new BigDecimal(1));
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", new BigDecimal(30));
        expectResult.add(expectMap);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("rows", 1);
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectResult1.add(expectMap1);

        Assertions.assertEquals(result.get(0), expectResult);
        Assertions.assertEquals(result.get(1), expectResult1);
        expectMap.put("AGE", new BigDecimal(31));
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
        expectMap.put("AGE", new BigDecimal(32));
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
        createTable(connection, dbType);
        DBManager.setAutoCommit(connection, dbType, false);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees
                """;
        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 1);

        boolean isRollBack = DBManager.rollback(connection, dbType);
        Assertions.assertTrue(isRollBack);

        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 0);

        DBManager.setAutoCommit(connection, dbType, true);

        // more test cases
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
        createTable(connection, dbType);
        insertData(connection, dbType);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEES");
        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", new BigDecimal(1));
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", new BigDecimal(30));
        expectResult.add(expectMap);
        Assertions.assertEquals(result, expectResult);

        createView(connection, dbType);
        List<Map<String, Object>> result2= DBManager.getTableOrViewData(connection, dbType, "M", "EMPLOYEE_VIEW");
        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("LAST_NAME", "Doe");
        expectMap2.put("FIRST_NAME", "John");
        expectMap2.put("EMAIL", "john.doe@example.com");
        expectResult2.add(expectMap2);
        Assertions.assertEquals(result2, expectResult2);

        // more test cases
        Assertions.assertEquals(DBManager.getTableOrViewData(connection, dbType, null, "EMPLOYEE_VIEW"), expectResult2);

        try {
            Assertions.assertEquals(expectResult2, DBManager.getTableOrViewData(connection, dbType, "fakeSchema", "EMPLOYEE_VIEW"));
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
        createTable(connection, dbType);
        insertData(connection, dbType);
        insertData1(connection, dbType);
        insertData2(connection, dbType);

        List<String> columnNames = new ArrayList<>();
        columnNames.add("FIRST_NAME");
        columnNames.add("LAST_NAME");
        columnNames.add("EMAIL");
        Map<String, Object> result = DBManager.getDataWithPage(connection, dbType, "M", "employees", columnNames, 1, 5);
        Assertions.assertEquals(result.get("totalPages"), 3);

        List<Map<String, Object>> resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 5);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        Assertions.assertEquals(resultList.get(0), expectMap);

        result = DBManager.getDataWithPage(connection, dbType, "M", "employees", new ArrayList<>(), 5, 1);
        Assertions.assertEquals(result.get("totalPages"), 12);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("ID", new BigDecimal(5));
        expectMap1.put("FIRST_NAME", "Charlie");
        expectMap1.put("LAST_NAME", "Williams");
        expectMap1.put("EMAIL", "charlie.williams@example.com");
        expectMap1.put("AGE", new BigDecimal(28));
        Assertions.assertEquals(resultList.get(0), expectMap1);

        result = DBManager.getDataWithPage(connection, dbType, "M", "employees", new ArrayList<>(), 1, 12);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("ID", new BigDecimal(12));
        expectMap2.put("FIRST_NAME", "Jane");
        expectMap2.put("LAST_NAME", "Jackson");
        expectMap2.put("EMAIL", "jane.jackson@example.com");
        expectMap2.put("AGE", new BigDecimal(36));
        Assertions.assertEquals(resultList.get(11), expectMap2);

        columnNames = new ArrayList<>();
        columnNames.add("id");
        columnNames.add("first_name");
        columnNames.add("last_name");
        columnNames.add("EMAIL");
        columnNames.add("age");
        result = DBManager.getDataWithPage(connection, dbType, "M", "employees", columnNames, 1, 100);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.get(11), expectMap2);

        result = DBManager.getDataWithPage(connection, dbType, "M", "employees", null, -1, 5);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        result = DBManager.getDataWithPage(connection, dbType, "M", "employees", null, 1, 0);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        // more test cases
        columnNames = new ArrayList<>();
        columnNames.add("first_name");
        columnNames.add("fake_column");
        try {
            DBManager.getDataWithPage(connection, dbType, "M", "employees", columnNames, 1, 5);
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

        result = DBManager.getDataWithPage(connection, dbType, "M", "employees", null, 3, 10);
        Assertions.assertEquals(result.get("totalPages"), 2);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 0);
    }

    @Test
    void testGetFunc() throws SQLException {
        createTable(connection, dbType);
        createFunc(connection, dbType);

        List<DatabaseFunction> result = DBManager.getFuncs(connection, dbType, "M");
        Assertions.assertEquals(result.size() > 1, true);

        DatabaseFunction function = new DatabaseFunction("M", "ADD_NUMBERS");
        Assertions.assertEquals(true, result.contains(function));

        // more test cases
        Assertions.assertTrue(DBManager.getFuncs(connection, dbType, null).size() > 1);
        Assertions.assertTrue(DBManager.getFuncs(connection, dbType, "fakeSchema").isEmpty());
    }

    @Test
    void testExecuteFunction() throws SQLException {
        createTable(connection, dbType);
        createFunc(connection, dbType);

        List<Map<String, Object>> result = DBManager.executeFunction(connection, dbType, "M", "ADD_NUMBERS", 10, 20);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("RESULT", new BigDecimal(30));
        List<Map<String, Object>> expect = new ArrayList<>();
        expect.add(expectMap);

        Assertions.assertEquals(expect, result);

        // more test cases
        try {
            DBManager.executeFunction(connection, dbType, "M", "fakeFunction", 10, 20);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeFunction(connection, dbType, "M", "add_numbers", 10);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeFunction(connection, dbType, "M", "add_numbers", 10, "abc");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        Assertions.assertEquals(expect, DBManager.executeFunction(connection, dbType, null, "add_numbers", 10, 20));
    }

    @Test
    void testMultiTypeData() throws SQLException, ClassNotFoundException {
        String sql = """
                CREATE TABLE MyTable
                (
                    MY_DECIMAL   NUMBER(10, 2),   -- Oracle uses NUMBER instead of DECIMAL
                    MY_DATE      DATE,            -- Date type
                    MY_TIME      INTERVAL DAY TO SECOND,  -- Oracle does not directly support TIME type, so INTERVAL DAY TO SECOND is used to store time
                    MY_TIME_STAMP TIMESTAMP,       -- Timestamp type
                    MY_BINARY    RAW(50)          -- Oracle uses RAW for binary data
                )
                """;
        String insertData = """
                INSERT INTO MyTable
                (
                    MY_DECIMAL,
                    MY_DATE,
                    MY_TIME,
                    MY_TIME_STAMP,
                    MY_BINARY
                )
                VALUES
                (
                    1234.56,                       -- Numeric value
                    TO_DATE('2023-05-31','YYYY-MM-DD'), -- Date
                    INTERVAL '12:34:56' HOUR TO SECOND,   -- Time
                    TO_TIMESTAMP('2023-05-31 12:34:56', 'YYYY-MM-DD HH24:MI:SS'), -- Timestamp
                    HEXTORAW('44424D61736B6572')  -- Binary data, inserting the Hex representation of "DBMasker"
                )
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(insertData);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "M", "MyTable");
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("MY_DECIMAL", new BigDecimal("1234.56"));

        LocalDate localDate = LocalDate.parse("2023-05-31");
        ZoneId defaultZoneId = ZoneId.systemDefault();
        Instant instant = localDate.atStartOfDay(defaultZoneId).toInstant();
        Date date = Date.from(instant);
        expectMap.put("MY_DATE", date);

        LocalTime time = LocalTime.parse("12:34:56");
        Duration duration = Duration.between(LocalTime.MIDNIGHT, time);
        expectMap.put("MY_TIME", INTERVALDS.toIntervalds(duration));


        expectMap.put("MY_TIME_STAMP", new TIMESTAMP(Timestamp.valueOf("2023-05-31 12:34:56")));

        expectMap.put("MY_BINARY", "DBMasker".getBytes());

        Assertions.assertEquals(expectMap.get("MY_DECIMAL"), result.get(0).get("MY_DECIMAL"));
        Assertions.assertEquals(expectMap.get("MY_DATE"), result.get(0).get("MY_DATE"));
        Assertions.assertEquals(expectMap.get("MY_TIME"), result.get(0).get("MY_TIME"));
        Assertions.assertEquals(expectMap.get("MY_TIME_STAMP"), result.get(0).get("MY_TIME_STAMP"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("MY_BINARY"), (byte[]) result.get(0).get("MY_BINARY"));
    }
}
