package com.dbmasker.database.mysql;

import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.data.*;
import com.dbmasker.database.DbType;
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
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

class MysqlAPITests {
    private String driver = "com.mysql.cj.jdbc.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.MYSQL.getDbName();
    private String version = "v8";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE employees (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL UNIQUE,
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

    public void createFunc(Connection connection, String dbType) throws SQLException {
        String sql2 = """
               CREATE FUNCTION add_numbers(a INT, b INT) RETURNS INT
               DETERMINISTIC
               BEGIN
                   RETURN a + b;
               END;
               """;
        DBManager.executeUpdateSQL(connection, dbType, sql2);
    }

    public void createDatabase(Connection connection, String dbType) throws SQLException {
        String sql = "CREATE DATABASE db_mysql_test";
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/mysql.properties")) {
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
                DROP TABLE IF EXISTS employees;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        String sql1 = "DROP VIEW IF EXISTS employee_view;";
        DBManager.executeUpdateSQL(connection, dbType, sql1);
        String sql2 = "DROP FUNCTION IF EXISTS add_numbers;";
        DBManager.executeUpdateSQL(connection, dbType, sql2);

        sql = """
                DROP TABLE IF EXISTS mytable;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);

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
        Connection connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertNotNull(connection);

        Assertions.assertTrue(DBManager.closeConnection(connection));

        // more test cases
        Assertions.assertTrue(connection.isClosed());
        Assertions.assertTrue(DBManager.closeConnection(connection));

        Assertions.assertFalse(DBManager.closeConnection(null));
    }

    @Test
    void testGetSchema() throws SQLException {
        List<String> sqliteSchemas = DBManager.getSchema(connection, dbType);
        Assertions.assertTrue(sqliteSchemas.contains("db_mysql_test"));
    }

    @Test
    void testGetTables() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<String> tables = DBManager.getTables(connection, dbType, "db_mysql_test");

        Assertions.assertEquals("employees", tables.get(0));

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
        Assertions.assertTrue(DBManager.getTables(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getTables(connection, dbType, ""));
    }

    @Test
    void testGetViews() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);
        createView(connection, dbType);

        List<String> sqliteViews = DBManager.getViews(connection, dbType, "db_mysql_test");
        List<String> expectViews = new ArrayList<>();
        expectViews.add("employee_view");

        Assertions.assertEquals(expectViews, sqliteViews);

        // more test cases
        Assertions.assertTrue(DBManager.getViews(connection, dbType, null).contains("employee_view"));
        Assertions.assertTrue(DBManager.getViews(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getViews(connection, dbType, ""));
    }

    @Test
    void testGetMetaData() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<TableMetaData> metaDatas = DBManager.getMetaData(connection, dbType, "db_mysql_test");
        Assertions.assertTrue(metaDatas.size() >= 5);

        // more test cases
        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, null).size() > 5);
        Assertions.assertEquals(0, DBManager.getMetaData(connection, dbType, "fakeSchema").size());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getMetaData(connection, dbType, ""));
    }

    @Test
    void testGetTableAttribute() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(tableAttributes.size(), 5);

        // more test cases
        Assertions.assertEquals(5, DBManager.getTableAttribute(connection, dbType, null, "employees").size());
        Assertions.assertTrue(DBManager.getTableAttribute(connection, dbType, "fakeSchema", "employees").isEmpty());

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
        createTable(connection, dbType);

        Set<String> tablePrimaryKeys = DBManager.getPrimaryKeys(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(1, tablePrimaryKeys.size());

        Set<String> expectPrimaryKeys = new HashSet<>();
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

        Assertions.assertEquals(0, DBManager.getPrimaryKeys(connection, dbType, "my_schema", "fakeTable").size());
    }

    @Test
    void testGetUniqueKey() throws SQLException {
        createTable(connection, dbType);

        Map<String, Set<String>> tableUniqueKey = DBManager.getUniqueKeys(connection, dbType, "db_mysql_test", "employees");
        Assertions.assertEquals(2, tableUniqueKey.size());

        // more test cases
        Assertions.assertEquals(2, DBManager.getUniqueKeys(connection, dbType, null, "employees").size());

        Assertions.assertTrue(DBManager.getUniqueKeys(connection, dbType, "fakeSchema", "employees").isEmpty());

        try {
            Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, null, null).size());
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertTrue(DBManager.getUniqueKeys(connection, dbType, null, "fakeTable").isEmpty());
    }

    @Test
    void testGetIndex() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);

        List<TableIndex> tableIndex = DBManager.getIndex(connection, dbType, "db_mysql_test", "employees");
        TableIndex tableIndex1 = new TableIndex("email", "email", true);
        TableIndex tableIndex2 = new TableIndex("PRIMARY", "id", true);
        List<TableIndex> expectIndex = new ArrayList<>();
        expectIndex.add(tableIndex1);
        expectIndex.add(tableIndex2);
        Assertions.assertEquals(expectIndex, tableIndex);

        // more test cases
        Assertions.assertEquals("email", DBManager.getIndex(connection, dbType, null, "employees").get(0).getColumnName());
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
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    age INT
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
            Assertions.assertEquals(1, DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees").size());
        }
    }

    @Test
    void testExecuteQuerySQL() throws SQLException, ClassNotFoundException {
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
    void testExecuteSQLScriptWithMask() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees;
                UPDATE employees SET age = 31 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM employees;
                """;

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
        obfuscationRule.setReplacement("[email redacted]");
        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("email", obfuscationRule);

        List<List<Map<String, Object>>> result = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 3);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "[email redacted]");
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

        // more test cases
        try {
            DBSecManager.execSQLScriptWithMask(connection, dbType, null, obfuscationRuleMap);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_SQL_SCRIPT_ERROR);
        }

        try {
            DBSecManager.execSQLScriptWithMask(connection, dbType, sql, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        // more test cases
        String sqlScript = """
                UPDATE employees SET age = 33 WHERE id = 1;
                fakeSQL;
                """;
        try {
            DBSecManager.execSQLScriptWithMask(connection, dbType, sqlScript, obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            sqlScript = """
                SELECT id, first_name, last_name, email, age FROM employees;
                """;
            result = DBSecManager.execSQLScriptWithMask(connection, dbType, sqlScript, obfuscationRuleMap);
            Assertions.assertEquals(result.get(0), expectResult);
        }

        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("last_name", "Doe");
        expectMap2.put("id", 1);
        expectMap2.put("first_name", "John");
        expectMap2.put("email", "john.doe@example.com");
        expectMap2.put("age", 31);
        expectResult2.add(expectMap2);

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        List<List<Map<String, Object>>> result2 = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(result2.size(), 3);
        Assertions.assertEquals(result2.get(0), expectResult2);
        Assertions.assertEquals(result2.get(1), expectResult1);
        Assertions.assertEquals(result2.get(2), expectResult2);


        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("email", obfuscationRule);
        result2 = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(result2.size(), 3);
        Assertions.assertEquals(result2.get(0), expectResult2);
        Assertions.assertEquals(result2.get(1), expectResult1);
        Assertions.assertEquals(result2.get(2), expectResult2);
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
                SELECT id, first_name, last_name, email, age FROM employees;
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

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employees");
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
        List<Map<String, Object>> result2= DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "employee_view");
        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("last_name", "Doe");
        expectMap2.put("first_name", "John");
        expectMap2.put("email", "john.doe@example.com");
        expectResult2.add(expectMap2);
        Assertions.assertEquals(result2, expectResult2);

        // more test cases
        Assertions.assertEquals(DBManager.getTableOrViewData(connection, dbType, null, "employee_view"), expectResult2);

        try {
            Assertions.assertEquals(expectResult2, DBManager.getTableOrViewData(connection, dbType, "fakeSchema", "employee_view"));
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
        columnNames.add("first_name");
        columnNames.add("last_name");
        columnNames.add("email");
        Map<String, Object> result = DBManager.getDataWithPage(connection, dbType, "db_mysql_test", "employees", columnNames, 1, 5);
        Assertions.assertEquals(result.get("totalPages"), 3);

        List<Map<String, Object>> resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 5);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        Assertions.assertEquals(resultList.get(0), expectMap);

        result = DBManager.getDataWithPage(connection, dbType, "db_mysql_test", "employees", new ArrayList<>(), 5, 1);
        Assertions.assertEquals(result.get("totalPages"), 12);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("id", 5);
        expectMap1.put("first_name", "Charlie");
        expectMap1.put("last_name", "Williams");
        expectMap1.put("email", "charlie.williams@example.com");
        expectMap1.put("age", 28);
        Assertions.assertEquals(resultList.get(0), expectMap1);

        result = DBManager.getDataWithPage(connection, dbType, "db_mysql_test", "employees", new ArrayList<>(), 1, 12);
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
        result = DBManager.getDataWithPage(connection, dbType, "db_mysql_test", "employees", columnNames, 1, 100);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.get(11), expectMap2);

        result = DBManager.getDataWithPage(connection, dbType, "db_mysql_test", "employees", null, -1, 5);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        result = DBManager.getDataWithPage(connection, dbType, "db_mysql_test", "employees", null, 1, 0);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        // more test cases
        columnNames = new ArrayList<>();
        columnNames.add("first_name");
        columnNames.add("fake_column");
        try {
            DBManager.getDataWithPage(connection, dbType, "db_mysql_test", "employees", columnNames, 1, 5);
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

        result = DBManager.getDataWithPage(connection, dbType, "db_mysql_test", "employees", null, 3, 10);
        Assertions.assertEquals(result.get("totalPages"), 2);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 0);
    }

    @Test
    void testGetFunc() throws SQLException {
        createTable(connection, dbType);
        createFunc(connection, dbType);

        List<DatabaseFunction> result = DBManager.getFuncs(connection, dbType, "db_mysql_test");
        Assertions.assertEquals(1, result.size());

        DatabaseFunction function = new DatabaseFunction("db_mysql_test", "add_numbers");
        List<DatabaseFunction> expect = new ArrayList<>();
        expect.add(function);
        Assertions.assertEquals(expect, result);

        // more test cases
        Assertions.assertTrue(DBManager.getFuncs(connection, dbType, null).size() > 1);
        Assertions.assertTrue(DBManager.getFuncs(connection, dbType, "fakeSchema").isEmpty());
    }

    @Test
    void testExecuteFunction() throws SQLException {
        createTable(connection, dbType);
        createFunc(connection, dbType);

        List<Map<String, Object>> result = DBManager.executeFunction(connection, dbType, "db_mysql_test", "add_numbers", 10, 20);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("db_mysql_test.add_numbers(10, 20)", 30);
        List<Map<String, Object>> expect = new ArrayList<>();
        expect.add(expectMap);

        Assertions.assertEquals(expect, result);

        // more test cases
        try {
            DBManager.executeFunction(connection, dbType, "db_mysql_test", "fakeFunction", 10, 20);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeFunction(connection, dbType, "db_mysql_test", "add_numbers", 10);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeFunction(connection, dbType, "db_mysql_test", "add_numbers", 10, "abc");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("add_numbers(10, 20)", 30);
        expect.clear();
        expect.add(expectMap1);
        Assertions.assertEquals(expect, DBManager.executeFunction(connection, dbType, null, "add_numbers", 10, 20));
    }

    @Test
    void testMultiTypeData() throws SQLException {
        String sql = """
                CREATE TABLE MyTable
                (
                    MyDecimal   DECIMAL(10, 2),  -- DECIMAL type, total length is 10, decimal places are 2
                    MyDate      DATE,            -- Date type
                    MyTime      TIME,            -- Time type
                    MyTimestamp TIMESTAMP,       -- Timestamp type
                    MyBoolean   BOOLEAN,         -- Boolean type, actually an alias for TINYINT type
                    MyBinary    BINARY(50),      -- BINARY type, maximum length is 50
                    MyBlob      BLOB             -- BLOB type
                );
                """;
        String insertData = """
                INSERT INTO MyTable
                (
                  MyDecimal,
                  MyDate,
                  MyTime,
                  MyTimestamp,
                  MyBoolean,
                  MyBinary,
                  MyBlob
                )
                VALUES
                (
                  1234.56,                  -- Numeric value
                  '2023-05-31',             -- Date
                  '12:34:56',               -- Time
                  '2023-05-31 12:34:56',    -- Timestamp
                  TRUE,                     -- Boolean value
                  UNHEX('44424D61736B6572'),-- Binary data, inserting the Hex representation of "DBMasker"
                  UNHEX('44424D61736B6572') -- BLOB data, inserting the Hex representation of "DBMasker"
                );
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(insertData);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "db_mysql_test", "MyTable");
        Assertions.assertEquals(result.size(), 1);
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("MyDecimal", new BigDecimal("1234.56"));

        LocalDate localDate = LocalDate.parse("2023-05-31");
        ZoneId defaultZoneId = ZoneId.systemDefault();
        Instant instant = localDate.atStartOfDay(defaultZoneId).toInstant();
        Date date = Date.from(instant);
        expectMap.put("MyDate", date);

        expectMap.put("MyTime", Time.valueOf("12:34:56"));
        expectMap.put("MyTimestamp", Timestamp.valueOf("2023-05-31 12:34:56"));
        expectMap.put("MyBoolean", true);

        byte[] newArray = new byte[50];
        System.arraycopy("DBMasker".getBytes(), 0, newArray, 0, "DBMasker".getBytes().length);
        expectMap.put("MyBinary", newArray);

        expectMap.put("MyBlob", "DBMasker".getBytes());

        Assertions.assertEquals(expectMap.get("MyDecimal"), result.get(0).get("MyDecimal"));
        Assertions.assertEquals(expectMap.get("MyDate"), result.get(0).get("MyDate"));
        Assertions.assertEquals(expectMap.get("MyTime"), result.get(0).get("MyTime"));
        Assertions.assertEquals(expectMap.get("MyTimestamp"), result.get(0).get("MyTimestamp"));
        Assertions.assertEquals(expectMap.get("MyBoolean"), result.get(0).get("MyBoolean"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("MyBinary"), (byte[]) result.get(0).get("MyBinary"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("MyBlob"), (byte[]) result.get(0).get("MyBlob"));
    }
}
