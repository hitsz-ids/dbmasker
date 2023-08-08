package com.dbmasker.database.hbase;

import com.dbmasker.api.DBManager;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.data.TableMetaData;
import com.dbmasker.database.DbType;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
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

class PhoenixAPITests {

    private String driver = "org.apache.phoenix.queryserver.client.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.PHOENIX.getDbName();
    private String version = "v1";

    private Connection connection;

    public void createSchema(Connection connection, String dbType) throws SQLException {
        List<String> sqlList = new ArrayList<>();
        String sql1 = "CREATE SCHEMA my_schema";

        String sql2 = """
                CREATE TABLE my_schema.employees (
                    id INTEGER NOT NULL primary key,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255),
                    age INTEGER
                )
                """;
        sqlList.add(sql1);
        sqlList.add(sql2);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);
    }

    public void createView(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE VIEW my_schema.employee_view AS
                SELECT * FROM my_schema.employees
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                upsert into my_schema.employees (id, first_name, last_name, email, age)
                values (1, 'John', 'Doe', 'john.doe@example.com', 30)
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(2, 'Jane', 'Smith', 'jane.smith@example.com', 28)";
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData2(Connection connection, String dbType) throws SQLException {
        String sql = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(3, 'Alice', 'Smith', 'alice.smith@example.com', 30)";
        String sql1 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(4, 'Bob', 'Johnson', 'bob.johnson@example.com', 35)";
        String sql2 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(5, 'Charlie', 'Williams', 'charlie.williams@example.com', 28)";
        String sql3 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(6, 'David', 'Brown', 'david.brown@example.com', 42)";
        String sql4 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(7, 'Eva', 'Jones', 'eva.jones@example.com', 26)";
        String sql5 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(8, 'Frank', 'Garcia', 'frank.garcia@example.com', 33)";
        String sql6 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(9, 'Grace', 'Martinez', 'grace.martinez@example.com', 29)";
        String sql7 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(10, 'Hannah', 'Anderson', 'hannah.anderson@example.com', 31)";
        String sql8 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(11, 'Ivan', 'Thomas', 'ivan.thomas@example.com', 27)";
        String sql9 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(12, 'Jane', 'Jackson', 'jane.jackson@example.com', 36)";
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(sql1);
        sqlList.add(sql2);
        sqlList.add(sql3);
        sqlList.add(sql4);
        sqlList.add(sql5);
        sqlList.add(sql6);
        sqlList.add(sql7);
        sqlList.add(sql8);
        sqlList.add(sql9);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/hbase.properties")) {
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
        DBManager.setAutoCommit(connection, dbType, true);
    }

    @AfterEach
    public void tearDown() throws SQLException {
        String sql3 = """
                DROP VIEW IF EXISTS my_schema.employee_view
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql3);
        String sql = """
                DROP TABLE IF EXISTS my_schema.employees
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        sql = """
                DROP TABLE IF EXISTS my_schema.my_table
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        String sql1 = "DROP SCHEMA IF EXISTS my_schema";
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
        Assertions.assertNotNull(DBManager.createConnection(driver, url, username, password));
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
    void testGetSchema() throws SQLException {
        // more test cases
        Assertions.assertTrue(!DBManager.getSchema(connection, dbType).contains("my_schema"));

        createSchema(connection, dbType);

        List<String> schemas = DBManager.getSchema(connection, dbType);
        Assertions.assertTrue(schemas.contains("MY_SCHEMA"));
    }

    @Test
    void testGetTables() throws SQLException, ClassNotFoundException {
        createSchema(connection, dbType);

        List<String> tables = DBManager.getTables(connection, dbType, "MY_SCHEMA");
        List<String> expectTables = new ArrayList<>();
        expectTables.add(" ");
        expectTables.add("EMPLOYEES");

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

        Assertions.assertTrue(DBManager.getTables(connection, dbType, null).contains("EMPLOYEES"));
        Assertions.assertTrue(DBManager.getTables(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        String sql = """
                  CREATE SCHEMA my_schema
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        Assertions.assertEquals(DBManager.getTables(connection, dbType, "MY_SCHEMA").get(0), " ");
    }

    @Test
    void testGetViews() throws SQLException, ClassNotFoundException {
        createSchema(connection, dbType);
        createView(connection, dbType);

        List<String> views = DBManager.getViews(connection, dbType, "MY_SCHEMA");
        List<String> expectViews = new ArrayList<>();
        expectViews.add("EMPLOYEE_VIEW");

        Assertions.assertEquals(expectViews, views);

        // more test cases
        Assertions.assertEquals(expectViews, DBManager.getViews(connection, dbType, null));
        Assertions.assertTrue(DBManager.getViews(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        String sql = """
                  CREATE SCHEMA my_schema
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getViews(connection, dbType, "MY_SCHEMA"));
    }

    @Test
    void testGetMetaData() throws SQLException, ClassNotFoundException {
        createSchema(connection, dbType);

        List<TableMetaData> metaDatas = DBManager.getMetaData(connection, dbType, "MY_SCHEMA");
        Assertions.assertEquals(metaDatas.size(), 5);

        // more test cases
        Assertions.assertEquals(DBManager.getMetaData(connection, dbType, null).size(), 5);
        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        String sql = """
                  CREATE SCHEMA my_schema
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getMetaData(connection, dbType, "MY_SCHEMA"));
    }

    @Test
    void testGetTableAttribute() throws SQLException {
        createSchema(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
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

        Assertions.assertTrue(DBManager.getTableAttribute(connection, dbType, null, "").size() > 5);
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
    void testExecuteUpdateSQL() throws SQLException {
        createSchema(connection, dbType);
        String insert = """
                upsert into my_schema.employees (id, first_name, last_name, email, age)
                values (1, 'John', 'Doe', 'john.doe@example.com', 30)
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

        Assertions.assertEquals(-1, DBManager.executeUpdateSQL(connection, dbType, "SELECT id, first_name, last_name, email, age FROM my_schema.employees"));
    }

    @Test
    void testExecuteUpdateSQLBatch() throws SQLException {
        List<String> sqlList = new ArrayList<>();
        String createSchema = """
                CREATE SCHEMA my_schema
                """;
        String createTable = """
                CREATE TABLE my_schema.employees (
                    id INTEGER NOT NULL primary key,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255),
                    age INTEGER
                )
                """;
        String createView = """
                CREATE VIEW my_schema.employee_view AS
                SELECT * FROM my_schema.employees
                """;
        String insert = """
                upsert into my_schema.employees (id, first_name, last_name, email, age)
                values (1, 'John', 'Doe', 'john.doe@example.com', 30)
                """;
        sqlList.add(createSchema);
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

        String insert2 = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(2, 'Jane', 'Smith', 'jane.smith@example.com', 28)";
        String fakeSQL = "fakeSQL";
        ArrayList<String> sqlList2 = new ArrayList<>();
        sqlList2.add(insert2);
        sqlList2.add(fakeSQL);
        try {
            DBManager.executeUpdateSQLBatch(connection, dbType, sqlList2);
            Assertions.fail();
        } catch (RuntimeException e) {
            Assertions.assertEquals(1, DBManager.getTableOrViewData(connection, dbType, "MY_SCHEMA", "EMPLOYEES").size());
        }
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees
                """;
        List<Map<String, Object>> result = DBManager.executeQuerySQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", 1);
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", 30);
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

        String insert = "upsert into my_schema.employees (id, first_name, last_name, email, age) values " +
                "(2, 'Jane', 'Smith', 'jane.smith@example.com', 28)";
        try {
            DBManager.executeQuerySQL(connection, dbType, insert);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testExecuteQuerySQLBatch() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees
                """;
        String sql2 = """
                SELECT first_name, last_name, email FROM my_schema.employees
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(sql2);

        List<List<Map<String, Object>>> result = DBManager.executeQuerySQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(result.size(), 2);

        Map<String, Object> expectMap1 = new HashMap<>();
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectMap1.put("LAST_NAME", "Doe");
        expectMap1.put("ID", 1);
        expectMap1.put("FIRST_NAME", "John");
        expectMap1.put("EMAIL", "john.doe@example.com");
        expectMap1.put("AGE", 30);
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
    void testExecuteSQL() throws SQLException, ClassNotFoundException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees
                """;
        List<Map<String, Object>> result = DBManager.executeSQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", 1);
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", 30);
        expectResult.add(expectMap);

        Assertions.assertEquals(result, expectResult);

        String updateSql = """
                upsert into my_schema.employees(id,age) values (1,31)
                """;
        result = DBManager.executeSQL(connection, dbType, updateSql);
        Assertions.assertEquals(result.size(), 1);


        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("rows", 1);
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectResult1.add(expectMap1);

        Assertions.assertEquals(result, expectResult1);

        try {
            DBManager.executeSQL(connection, dbType, "fakeSQL");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testExecuteSQLBatch() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        String sql1 = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees
                """;
        String sql2 = """
                upsert into my_schema.employees(id,age) values (1,31)
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql1);
        sqlList.add(sql2);

        List<List<Map<String, Object>>> result = DBManager.executeSQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(result.size(), 2);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", 1);
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", 30);
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
                upsert into my_schema.employees(id,age) values (1,32)
                """;
        sqlList = new ArrayList<>();
        sqlList.add(sql2);
        sqlList.add(sql1);

        result = DBManager.executeSQLBatch(connection, dbType, sqlList);
        expectResultList = new ArrayList<>();
        expectResultList.add(expectResult1);
        expectMap.put("AGE", 31);
        expectResultList.add(expectResult);

        Assertions.assertEquals(result, expectResultList);
    }

    @Test
    void testExecuteSQLScript() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        String sqlScript = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                upsert into my_schema.employees(id,age) values (1,31);
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;

        List<List<Map<String, Object>>> result = DBManager.executeSQLScript(connection, dbType, sqlScript);
        Assertions.assertEquals(result.size(), 3);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", 1);
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", 30);
        expectResult.add(expectMap);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("rows", 1);
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectResult1.add(expectMap1);

        Assertions.assertEquals(result.get(0), expectResult);
        Assertions.assertEquals(result.get(1), expectResult1);
        Assertions.assertEquals(result.get(2), expectResult);

        sqlScript = """
                upsert into my_schema.employees(id,age) values (1,31);
                upsert into my_schema.employees(id,age) values (1,32);
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;

        result = DBManager.executeSQLScript(connection, dbType, sqlScript);
        Assertions.assertEquals(result.size(), 3);

        expectMap.put("AGE", 31);
        Assertions.assertEquals(result.get(1), expectResult1);
        Assertions.assertEquals(result.get(0), expectResult1);
        Assertions.assertEquals(result.get(2), expectResult);

        expectMap.put("AGE", 32);
        // more test cases
        sqlScript = """
                upsert into my_schema.employees(id,age) values (1,33);
                fakeSQL;
                """;
        try {
            DBManager.executeSQLScript(connection, dbType, sqlScript);
            Assertions.fail();
        } catch (SQLException e) {
            sqlScript = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
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
    void testCommit() throws SQLException {
        createSchema(connection, dbType);

        boolean setAutoCommit = DBManager.setAutoCommit(connection, dbType, false);
        Assertions.assertTrue(setAutoCommit);

        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees
                """;
        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 0);

        boolean isCommitted = DBManager.commit(connection, dbType);
        Assertions.assertTrue(isCommitted);

        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 1);

        DBManager.setAutoCommit(connection, dbType, true);
    }

    @Test
    void testRollBack() throws SQLException, ClassNotFoundException {
        createSchema(connection, dbType);

        DBManager.setAutoCommit(connection, dbType, false);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees
                """;
        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 0);

        boolean isRollBack = DBManager.rollback(connection, dbType);
        Assertions.assertTrue(isRollBack);

        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 0);

        DBManager.setAutoCommit(connection, dbType, true);

        // more test cases
        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        DBManager.setAutoCommit(connection, dbType, true);
        createSchema(connection, dbType);
        insertData(connection, dbType);

        isRollBack = DBManager.rollback(connection, dbType);
        Assertions.assertFalse(isRollBack);

        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 1);
    }

    @Test
    void testGetTableData() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", 1);
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        expectMap.put("AGE", 30);
        expectResult.add(expectMap);
        Assertions.assertEquals(result, expectResult);

        createView(connection, dbType);
        List<Map<String, Object>> result2= DBManager.getTableOrViewData(connection, dbType, "my_schema", "employee_view");
        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("ID", 1);
        expectMap2.put("LAST_NAME", "Doe");
        expectMap2.put("FIRST_NAME", "John");
        expectMap2.put("EMAIL", "john.doe@example.com");
        expectMap2.put("AGE", 30);
        expectResult2.add(expectMap2);
        Assertions.assertEquals(result2, expectResult2);

        // more test cases
        try {
            DBManager.getTableOrViewData(connection, dbType, null, "employees");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

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
        createSchema(connection, dbType);
        insertData(connection, dbType);
        insertData1(connection, dbType);
        insertData2(connection, dbType);


        List<String> columnNames = new ArrayList<>();
        columnNames.add("FIRST_NAME");
        columnNames.add("LAST_NAME");
        columnNames.add("EMAIL");
        Map<String, Object> result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", columnNames, 1, 5);
        Assertions.assertEquals(result.get("totalPages"), 3);

        List<Map<String, Object>> resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 5);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.doe@example.com");
        Assertions.assertEquals(resultList.get(0), expectMap);

        result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", new ArrayList<>(), 5, 1);
        Assertions.assertEquals(result.get("totalPages"), 12);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("ID", 5);
        expectMap1.put("FIRST_NAME", "Charlie");
        expectMap1.put("LAST_NAME", "Williams");
        expectMap1.put("EMAIL", "charlie.williams@example.com");
        expectMap1.put("AGE", 28);
        Assertions.assertEquals(resultList.get(0), expectMap1);

        result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", new ArrayList<>(), 1, 12);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("ID", 12);
        expectMap2.put("FIRST_NAME", "Jane");
        expectMap2.put("LAST_NAME", "Jackson");
        expectMap2.put("EMAIL", "jane.jackson@example.com");
        expectMap2.put("AGE", 36);
        Assertions.assertEquals(resultList.get(11), expectMap2);

        columnNames = new ArrayList<>();
        columnNames.add("ID");
        columnNames.add("FIRST_NAME");
        columnNames.add("LAST_NAME");
        columnNames.add("EMAIL");
        columnNames.add("AGE");
        result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", columnNames, 1, 100);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.get(11), expectMap2);

        result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", null, -1, 5);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", null, 1, 0);
        Assertions.assertEquals(result.get("totalPages"), 1);

        resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 12);

        // more test cases
        columnNames = new ArrayList<>();
        columnNames.add("FIRST_NAME");
        columnNames.add("FAKE_COLUMN");
        try {
            DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", columnNames, 1, 5);
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

        result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", null, 3, 10);
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
        createSchema(connection, dbType);
        String sql = """
                CREATE TABLE my_schema.my_table
                (
                  my_decimal   DECIMAL(10,2)  NOT NULL primary key,  -- Phoenix also support DECIMAL type
                  my_date      DATE,           -- Phoenix support DATE type
                  my_time      TIME,           -- Phoenix support TIME type
                  my_timestamp TIMESTAMP,      -- Phoenix supports TIMESTAMP type
                  my_boolean   BOOLEAN,        -- Phoenix supports BOOLEAN type
                  my_blob      VARBINARY       -- BLOB can be represented as VARBINARY in Phoenix
                )
                """;
        String insertData = """
                UPSERT INTO my_schema.my_table
                (
                  my_decimal,
                  my_date,
                  my_time,
                  my_timestamp,
                  my_boolean,
                  my_blob
                )
                VALUES
                (
                  1234.56,
                  TO_DATE('2023-05-31', 'yyyy-MM-dd'),
                  TO_TIME('12:34:56', 'HH:mm:ss'),
                  TO_TIMESTAMP('2023-05-31 12:34:56', 'yyyy-MM-dd HH:mm:ss'),
                  TRUE,
                  'DBMasker'  -- 'DBMasker'
                )
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(insertData);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "my_table");
        Assertions.assertEquals(result.size(), 1);
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("my_decimal", new BigDecimal("1234.56"));

        LocalDate localDate = LocalDate.parse("2023-05-31");
        ZoneId defaultZoneId = ZoneId.systemDefault();
        Instant instant = localDate.atStartOfDay(defaultZoneId).toInstant();
        Date date = Date.from(instant);
        expectMap.put("my_date", date);

        expectMap.put("my_time", Time.valueOf("12:34:56"));
        expectMap.put("my_timestamp", Timestamp.valueOf("2023-05-31 12:34:56"));
        expectMap.put("my_boolean", true);

        expectMap.put("my_blob", "DBMasker".getBytes());

        Assertions.assertEquals(expectMap.get("my_decimal"), result.get(0).get("MY_DECIMAL"));
        Assertions.assertEquals(expectMap.get("my_date"), result.get(0).get("MY_DATE"));
        Assertions.assertEquals(expectMap.get("my_time"), result.get(0).get("MY_TIME"));
        Assertions.assertEquals(expectMap.get("my_timestamp"), result.get(0).get("MY_TIMESTAMP"));
        Assertions.assertEquals(expectMap.get("my_boolean"), result.get(0).get("MY_BOOLEAN"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("my_blob"), (byte[]) result.get(0).get("MY_BLOB"));
    }
}
