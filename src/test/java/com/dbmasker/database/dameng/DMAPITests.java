package com.dbmasker.database.dameng;

import com.dbmasker.api.DBManager;
import com.dbmasker.data.DatabaseFunction;
import com.dbmasker.data.TableAttribute;
import com.dbmasker.data.TableIndex;
import com.dbmasker.data.TableMetaData;
import com.dbmasker.database.DbType;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import dm.jdbc.driver.DmdbBlob;
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

class DMAPITests {
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
                 CREATE TABLE my_schema.employees (
                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    age INTEGER
                 );
                 """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void createView(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE VIEW my_schema.employee_view AS
                SELECT first_name, last_name, email
                FROM my_schema.employees;
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.employees (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData1(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.employees (first_name, last_name, email, age) 
                VALUES ('Jane', 'Smith', 'jane.smith@example.com', 28);
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData2(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO my_schema.employees (first_name, last_name, email, age) VALUES
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
        String sql = """
                CREATE 
                    FUNCTION my_schema.add_numbers
                        (a INTEGER, b INTEGER)
                        RETURN INTEGER
                    AS
                        s INTEGER;
                    BEGIN
                        s:=a+b;
                        RETURN s;
                    END;
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
    void testGetSchema() throws SQLException, ClassNotFoundException {
        List<String> schemas = DBManager.getSchema(connection, dbType);
        Assertions.assertTrue(schemas.contains("MY_SCHEMA"));

        // more test cases
        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        Assertions.assertTrue(!DBManager.getSchema(connection, dbType).contains("my_schema"));
        createSchema(connection, dbType);
    }

    @Test
    void testGetTables() throws SQLException, ClassNotFoundException {
        List<String> tables = DBManager.getTables(connection, dbType, "MY_SCHEMA");
        List<String> expectTables = new ArrayList<>();
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

        tables = DBManager.getTables(connection, dbType, null);
        Assertions.assertTrue(tables.contains("EMPLOYEES"));
        Assertions.assertTrue(DBManager.getTables(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        String sql = """
                  CREATE SCHEMA my_schema;
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getTables(connection, dbType, "my_schema"));
    }

    @Test
    void testGetViews() throws SQLException, ClassNotFoundException {
        createView(connection, dbType);

        List<String> resultViews = DBManager.getViews(connection, dbType, "MY_SCHEMA");
        List<String> expectViews = new ArrayList<>();
        expectViews.add("EMPLOYEE_VIEW");

        Assertions.assertEquals(expectViews, resultViews);

        // more test cases
        Assertions.assertTrue(DBManager.getViews(connection, dbType, null).contains("EMPLOYEE_VIEW"));
        Assertions.assertTrue(DBManager.getViews(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        String sql = """
                  CREATE SCHEMA my_schema;
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getViews(connection, dbType, "my_schema"));
    }

    @Test
    void testGetMetaData() throws SQLException, ClassNotFoundException {
        List<TableMetaData> metaDatas = DBManager.getMetaData(connection, dbType, "MY_SCHEMA");
        Assertions.assertEquals(5, metaDatas.size());

        // more test cases
        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, null).size() > 5);
        Assertions.assertTrue( DBManager.getMetaData(connection, dbType, "fakeSchema").isEmpty());

        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        String sql = """
                  CREATE SCHEMA my_schema;
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
        Assertions.assertEquals(new ArrayList<>(), DBManager.getMetaData(connection, dbType, "my_schema"));
    }

    @Test
    void testGetTableAttribute() throws SQLException {
        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(5, tableAttributes.size());

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
        Set<String> tablePrimaryKeys = DBManager.getPrimaryKeys(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(1, tablePrimaryKeys.size());

        Set<String> expectPrimaryKeys = new HashSet<>();
        expectPrimaryKeys.add("ID");

        Assertions.assertEquals(expectPrimaryKeys, tablePrimaryKeys);

        // more test cases
        Assertions.assertEquals(0, DBManager.getPrimaryKeys(connection, dbType, null, "EMPLOYEES").size());
        Assertions.assertEquals(0, DBManager.getPrimaryKeys(connection, dbType, "fakeSchema", "EMPLOYEES").size());

        try {
            DBManager.getPrimaryKeys(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getPrimaryKeys(connection, dbType, "MY_SCHEMA", "fakeTable").size());
    }

    @Test
    void testGetUniqueKey() throws SQLException {
        Map<String, Set<String>> tableUniqueKey = DBManager.getUniqueKeys(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        Assertions.assertEquals(2, tableUniqueKey.size());

        // more test cases
        Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, null, "EMPLOYEES").size());
        Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, "fakeSchema", "EMPLOYEES").size());

        try {
            DBManager.getUniqueKeys(connection, dbType, null, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, null, "fakeTable").size());
    }

    @Test
    void testGetIndex() throws SQLException {
        List<TableIndex> tableIndex = DBManager.getIndex(connection, dbType, "MY_SCHEMA", "EMPLOYEES");
        TableIndex tableIndex1 = new TableIndex("", "EMAIL", true);
        TableIndex tableIndex2 = new TableIndex("", "ID", true);
        List<TableIndex> expectIndex = new ArrayList<>();
        expectIndex.add(tableIndex1);
        expectIndex.add(tableIndex2);
        Assertions.assertEquals(tableIndex.size(), expectIndex.size());

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
        String insert = """
                INSERT INTO my_schema.employees (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        int rowCount = DBManager.executeUpdateSQL(connection, dbType, insert);
        Assertions.assertEquals(1, rowCount);

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
        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);

        List<String> sqlList = new ArrayList<>();
        String createSchema = """
                CREATE SCHEMA my_schema;
                """;
        String createTable = """
                CREATE TABLE my_schema.employees1 (
                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    age INTEGER
                 );
                """;
        String createView = """
                CREATE VIEW my_schema.employee_view AS
                SELECT first_name, last_name, email
                FROM my_schema.employees1;
                """;
        String insert = """
                INSERT INTO my_schema.employees1 (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        sqlList.add(createSchema);
        sqlList.add(createTable);
        sqlList.add(createView);
        sqlList.add(insert);

        int rowCount = DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(1, rowCount);

        // more test cases
        try {
            DBManager.executeUpdateSQLBatch(connection, dbType, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_SQL_LIST_ERROR));
        }

        Assertions.assertEquals(0, DBManager.executeUpdateSQLBatch(connection, dbType, new ArrayList<>()));

        String insert2 = """
                INSERT INTO my_schema.employees (first_name, last_name, email, age) 
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
            Assertions.assertEquals(1, DBManager.getTableOrViewData(connection, dbType, "my_schema", "EMPLOYEES1").size());
        }
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;
        List<Map<String, Object>> result = DBManager.executeQuerySQL(connection, dbType, sql);
        Assertions.assertEquals(1, result.size());

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

        String insert = """
                INSERT INTO my_schema.employees (first_name, last_name, email, age) 
                VALUES ('Jane', 'Smith', 'jane.smith@example.com', 28);
                """;
        Assertions.assertTrue(DBManager.executeQuerySQL(connection, dbType, insert).isEmpty());
    }

    @Test
    void testExecuteQuerySQLBatch() throws SQLException {
        insertData(connection, dbType);
        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;
        String sql2 = """
                SELECT first_name, last_name, email FROM my_schema.employees;
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(sql2);

        List<List<Map<String, Object>>> result = DBManager.executeQuerySQLBatch(connection, dbType, sqlList);
        Assertions.assertEquals(2, result.size());

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
        Assertions.assertEquals(3, DBManager.executeQuerySQLBatch(connection, dbType, sqlList).size());
    }

    @Test
    void testExecuteSQL() throws SQLException, ClassNotFoundException {
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;
        List<Map<String, Object>> result = DBManager.executeSQL(connection, dbType, sql);
        Assertions.assertEquals(1, result.size());

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
                UPDATE my_schema.employees SET age = 31 WHERE id = 1;
                """;
        result = DBManager.executeSQL(connection, dbType, updateSql);
        Assertions.assertEquals(1, result.size());

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("rows", 1);
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectResult1.add(expectMap1);

        Assertions.assertEquals(result, expectResult1);

        // more test cases
        sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                UPDATE my_schema.employees SET age = 32 WHERE id = 1;
                """;
        result = DBManager.executeSQL(connection, dbType, sql);
        Assertions.assertEquals(result.size(), 1);
        expectMap.put("AGE", 31);
        Assertions.assertEquals(result, expectResult);

        sql = """
                UPDATE my_schema.employees SET age = 32 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
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
        insertData(connection, dbType);

        String sql1 = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;
        String sql2 = """
                UPDATE my_schema.employees SET age = 31 WHERE id = 1;
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
                UPDATE my_schema.employees SET age = 32 WHERE id = 1;
                """;
        sqlList = new ArrayList<>();
        sqlList.add(sql2);
        sqlList.add(sql1);

        result = DBManager.executeSQLBatch(connection, dbType, sqlList);
        expectResultList = new ArrayList<>();
        expectResultList.add(expectResult1);
        expectMap.put("AGE", 32);
        expectResultList.add(expectResult);

        Assertions.assertEquals(result, expectResultList);
    }

    @Test
    void testExecuteSQLScript() throws SQLException {
        insertData(connection, dbType);

        String sqlScript = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                UPDATE my_schema.employees SET age = 31 WHERE id = 1;
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
        expectMap.put("AGE", 31);
        Assertions.assertEquals(result.get(2), expectResult);

        sqlScript = """
                UPDATE my_schema.employees SET age = 31 WHERE id = 1;
                UPDATE my_schema.employees SET age = 32 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;

        result = DBManager.executeSQLScript(connection, dbType, sqlScript);
        Assertions.assertEquals(result.size(), 3);

        Assertions.assertEquals(result.get(1), expectResult1);
        Assertions.assertEquals(result.get(0), expectResult1);
        expectMap.put("AGE", 32);
        Assertions.assertEquals(result.get(2), expectResult);

        // more test cases
        sqlScript = """
                UPDATE my_schema.employees SET age = 33 WHERE id = 1;
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
        boolean setAutoCommit = DBManager.setAutoCommit(connection, dbType, false);
        Assertions.assertTrue(setAutoCommit);

        insertData(connection, dbType);

        boolean isCommitted = DBManager.commit(connection, dbType);
        Assertions.assertTrue(isCommitted);

        DBManager.setAutoCommit(connection, dbType, true);
    }

    @Test
    void testRollBack() throws SQLException, ClassNotFoundException {
        DBManager.setAutoCommit(connection, dbType, false);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;
        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 1);

        boolean isRollBack = DBManager.rollback(connection, dbType);
        Assertions.assertTrue(isRollBack);

        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 0);

        DBManager.setAutoCommit(connection, dbType, true);

        // more test cases
        tearDown();
        connection = DBManager.createConnection(driver, url, username, password);
        createSchema(connection, dbType);
        insertData(connection, dbType);

        isRollBack = DBManager.rollback(connection, dbType);
        Assertions.assertFalse(isRollBack);

        Assertions.assertEquals(DBManager.executeQuerySQL(connection, dbType, sql).size(), 1);
    }

    @Test
    void testGetTableData() throws SQLException {
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
        expectMap2.put("LAST_NAME", "Doe");
        expectMap2.put("FIRST_NAME", "John");
        expectMap2.put("EMAIL", "john.doe@example.com");
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
    void testGetFunc() throws SQLException {
        createFunc(connection, dbType);

        List<DatabaseFunction> result = DBManager.getFuncs(connection, dbType, "MY_SCHEMA");
        Assertions.assertEquals(1, result.size());

        DatabaseFunction function = new DatabaseFunction("MY_SCHEMA", "ADD_NUMBERS");
        List<DatabaseFunction> expect = new ArrayList<>();
        expect.add(function);
        Assertions.assertEquals(expect, result);

        // more test cases
        Assertions.assertTrue(DBManager.getFuncs(connection, dbType, null).size() >= 1);
        Assertions.assertTrue(DBManager.getFuncs(connection, dbType, "fakeSchema").isEmpty());
    }

    @Test
    void testExecuteFunction() throws SQLException {
        createFunc(connection, dbType);

        List<Map<String, Object>> result = DBManager.executeFunction(connection, dbType, "MY_SCHEMA", "ADD_NUMBERS", 10, 20);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("MY_SCHEMA.ADD_NUMBERS(:<1>,:<2>)", 30);
        List<Map<String, Object>> expect = new ArrayList<>();
        expect.add(expectMap);

        Assertions.assertEquals(expect, result);

        // more test cases
        try {
            DBManager.executeFunction(connection, dbType, "my_schema", "fakeFunction", 10, 20);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeFunction(connection, dbType, "my_schema", "add_numbers", 10);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeFunction(connection, dbType, "my_schema", "add_numbers", 10, "abc");
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBManager.executeFunction(connection, dbType, null, "add_numbers", 10, 20);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }
    }

    @Test
    void testMultiTypeData() throws SQLException {
        String sql = """
                CREATE TABLE my_schema.my_table
                (
                    my_decimal   DECIMAL(10, 2),  -- DECIMAL type, total length is 10, decimal places are 2
                    my_date      DATE,            -- Date type
                    my_time      TIME,            -- Time type
                    my_timestamp TIMESTAMP,       -- Timestamp type
                    my_binary    BINARY(50),      -- BINARY type, maximum length is 50
                    my_blob      BLOB             -- BLOB type
                );
                """;
        String insertData = """
                INSERT INTO my_schema.my_table
                (
                  my_decimal,
                  my_date,
                  my_time,
                  my_timestamp,
                  my_binary,
                  my_blob
                )
                VALUES
                (
                  1234.56,                           -- Numeric value
                  DATE '2023-05-31',                 -- Date
                  TIME '12:34:56',                   -- Time
                  TIMESTAMP '2023-05-31 12:34:56',   -- Timestamp
                  0x44424D61736B6572,                -- Binary data, inserting hex representation of "DBMasker"
                  0x44424D61736B6572                 -- BLOB data, inserting hex representation of "DBMasker"
                );
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(insertData);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "my_table");
        Assertions.assertEquals(result.size(), 1);
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("MY_DECIMAL", new BigDecimal("1234.56"));

        LocalDate localDate = LocalDate.parse("2023-05-31");
        ZoneId defaultZoneId = ZoneId.systemDefault();
        Instant instant = localDate.atStartOfDay(defaultZoneId).toInstant();
        Date date = Date.from(instant);
        expectMap.put("MY_DATE", date);

        expectMap.put("MY_TIME", Time.valueOf("12:34:56"));
        expectMap.put("MY_TIMESTAMP", Timestamp.valueOf("2023-05-31 12:34:56"));

        byte[] newArray = new byte[50];
        System.arraycopy("DBMasker".getBytes(), 0, newArray, 0, "DBMasker".getBytes().length);
        expectMap.put("MY_BINARY", newArray);

        expectMap.put("MY_BLOB", "DBMasker".getBytes());

        Assertions.assertEquals(expectMap.get("MY_DECIMAL"), result.get(0).get("MY_DECIMAL"));
        Assertions.assertEquals(expectMap.get("MY_DATE"), result.get(0).get("MY_DATE"));
        Assertions.assertEquals(expectMap.get("MY_TIME"), result.get(0).get("MY_TIME"));
        Assertions.assertEquals(expectMap.get("MY_TIMESTAMP"), result.get(0).get("MY_TIMESTAMP"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("MY_BINARY"), (byte[]) result.get(0).get("MY_BINARY"));

        DmdbBlob blob = (DmdbBlob) result.get(0).get("MY_BLOB");
        Assertions.assertArrayEquals((byte[]) expectMap.get("MY_BLOB"), blob.getBytes(1, (int)blob.length()));
    }

}
