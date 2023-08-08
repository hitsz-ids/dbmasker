package com.dbmasker.database.postgresql;

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

class PostgreSQLAPITests {
    private String driver = "org.postgresql.Driver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.POSTGRESQL.getDbName();
    private String version = "v9";

    private Connection connection;

    public void createSchema(Connection connection, String dbType) throws SQLException {
        String sql = """
                  CREATE SCHEMA my_schema;
                  CREATE SEQUENCE my_schema.employees_id_seq;
                  
                  CREATE TABLE my_schema.employees (
                      id INTEGER PRIMARY KEY DEFAULT nextval('my_schema.employees_id_seq'),
                      first_name VARCHAR(255) NOT NULL,
                      last_name VARCHAR(255) NOT NULL,
                      email VARCHAR(255) NOT NULL UNIQUE,
                      age INTEGER
                  );
                  """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }
    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE TABLE employees (
                      id INTEGER PRIMARY KEY DEFAULT nextval('my_schema.employees_id_seq'),
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
                CREATE OR REPLACE FUNCTION my_schema.add_numbers
                    (a INTEGER, b INTEGER)
                    RETURNS INTEGER
                AS $$
                DECLARE
                    s INTEGER;
                BEGIN
                    s := a + b;
                    RETURN s;
                END;
                $$ LANGUAGE plpgsql;
                """;
        DBManager.executeSQLScript(connection, dbType, sql);
    }

    public void createDatabase(Connection connection, String dbType) throws SQLException {
        String sql = "CREATE DATABASE db_postgresql_test;";
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/postgreSQL.properties")) {
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

        String sql1 = "DROP SCHEMA IF EXISTS my_schema CASCADE;";
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
        // more test cases
        Assertions.assertTrue(!DBManager.getSchema(connection, dbType).contains("my_schema"));

        createSchema(connection, dbType);
        List<String> sqliteSchemas = DBManager.getSchema(connection, dbType);
        Assertions.assertTrue(sqliteSchemas.contains("my_schema"));
    }

    @Test
    void testGetTables() throws SQLException, ClassNotFoundException {
        createSchema(connection, dbType);

        List<String> tables = DBManager.getTables(connection, dbType, "my_schema");
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
        createSchema(connection, dbType);
        createView(connection, dbType);

        List<String> views = DBManager.getViews(connection, dbType, "my_schema");
        List<String> expectViews = new ArrayList<>();
        expectViews.add("employee_view");

        Assertions.assertEquals(expectViews, views);

        // more test cases
        Assertions.assertEquals(expectViews, DBManager.getViews(connection, dbType, null));
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
        createSchema(connection, dbType);

        List<TableMetaData> metaDatas = DBManager.getMetaData(connection, dbType, "my_schema");
        Assertions.assertEquals(metaDatas.size(), 5);

        // more test cases
        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, null).size() >= 5);
        Assertions.assertTrue(DBManager.getMetaData(connection, dbType, "fakeSchema").isEmpty());

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
        createSchema(connection, dbType);

        List<TableAttribute> tableAttributes = DBManager.getTableAttribute(connection, dbType, "my_schema", "employees");
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

        Assertions.assertTrue(DBManager.getTableAttribute(connection, dbType, null, "").size() > 5);
    }

    @Test
    void testGetPrimaryKeys() throws SQLException, ClassNotFoundException {
        createSchema(connection, dbType);

        List<String> tablePrimaryKeys = DBManager.getPrimaryKeys(connection, dbType, "my_schema", "employees");
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

        Assertions.assertEquals(0, DBManager.getPrimaryKeys(connection, dbType, "my_schema", "fakeTable").size());
    }

    @Test
    void testGetUniqueKey() throws SQLException {
        createSchema(connection, dbType);

        Map<String, Set<String>> tableUniqueKey = DBManager.getUniqueKeys(connection, dbType, "my_schema", "employees");
        Assertions.assertEquals(tableUniqueKey.size(), 2);

        // more test cases
        Assertions.assertEquals(2, DBManager.getUniqueKeys(connection, dbType, null, "employees").size());
        Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, "fakeSchema", "employees").size());

        try {
            Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, null, null).size());
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertTrue(e.getMessage().startsWith(ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR));
        }

        Assertions.assertEquals(0, DBManager.getUniqueKeys(connection, dbType, null, "fakeTable").size());
    }

    @Test
    void testGetIndex() throws SQLException {
        createSchema(connection, dbType);

        List<TableIndex> tableIndex = DBManager.getIndex(connection, dbType, "my_schema", "employees");
        TableIndex tableIndex1 = new TableIndex("employees_email_key", "email", true);
        TableIndex tableIndex2 = new TableIndex("employees_pkey", "id", true);
        List<TableIndex> expectIndex = new ArrayList<>();
        expectIndex.add(tableIndex1);
        expectIndex.add(tableIndex2);
        Assertions.assertEquals(expectIndex, tableIndex);

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
        createSchema(connection, dbType);
        String insert = """
                INSERT INTO my_schema.employees (first_name, last_name, email, age)
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
    void testExecuteUpdateSQLBatch() throws SQLException {
        List<String> sqlList = new ArrayList<>();
        String createSchema = """
                CREATE SCHEMA my_schema;
                """;
        String createSeq = "CREATE SEQUENCE my_schema.employees_id_seq;";
        String createTable = """
                CREATE TABLE my_schema.employees (
                  id INTEGER PRIMARY KEY DEFAULT nextval('my_schema.employees_id_seq'),
                  first_name VARCHAR(255) NOT NULL,
                  last_name VARCHAR(255) NOT NULL,
                  email VARCHAR(255) NOT NULL UNIQUE,
                  age INTEGER
                );
                """;
        String createView = """
                CREATE VIEW my_schema.employee_view AS
                SELECT first_name, last_name, email
                FROM my_schema.employees;
                """;
        String insert = """
                INSERT INTO my_schema.employees (first_name, last_name, email, age)
                VALUES ('John', 'Doe', 'john.doe@example.com', 30);
                """;
        sqlList.add(createSchema);
        sqlList.add(createSeq);
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
            Assertions.assertEquals(1, DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees").size());
        }
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
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
                INSERT INTO my_schema.employees (first_name, last_name, email, age) 
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
        createSchema(connection, dbType);
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
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
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
                UPDATE my_schema.employees SET age = 31 WHERE id = 1;
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
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                UPDATE my_schema.employees SET age = 32 WHERE id = 1;
                """;
        try {
            DBManager.executeSQL(connection, dbType, sql);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

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
        createSchema(connection, dbType);
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
                UPDATE my_schema.employees SET age = 32 WHERE id = 1;
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
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                UPDATE my_schema.employees SET age = 31 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
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
                UPDATE my_schema.employees SET age = 31 WHERE id = 1;
                UPDATE my_schema.employees SET age = 32 WHERE id = 1;
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;

        result = DBManager.executeSQLScript(connection, dbType, sqlScript);
        Assertions.assertEquals(result.size(), 3);

        Assertions.assertEquals(result.get(1), expectResult1);
        Assertions.assertEquals(result.get(0), expectResult1);
        expectMap.put("age", 32);
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
        createSchema(connection, dbType);
        boolean setAutoCommit = DBManager.setAutoCommit(connection, dbType, false);
        Assertions.assertTrue(setAutoCommit);

        insertData(connection, dbType);

        boolean isCommitted = DBManager.commit(connection, dbType);
        Assertions.assertTrue(isCommitted);

        DBManager.setAutoCommit(connection, dbType, true);
    }

    @Test
    void testRollBack() throws SQLException, ClassNotFoundException {
        createSchema(connection, dbType);
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
        createSchema(connection, dbType);
        insertData(connection, dbType);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "employees");
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
        List<Map<String, Object>> result2= DBManager.getTableOrViewData(connection, dbType, "my_schema", "employee_view");
        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("last_name", "Doe");
        expectMap2.put("first_name", "John");
        expectMap2.put("email", "john.doe@example.com");
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
        columnNames.add("first_name");
        columnNames.add("last_name");
        columnNames.add("email");
        Map<String, Object> result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", columnNames, 1, 5);
        Assertions.assertEquals(result.get("totalPages"), 3);

        List<Map<String, Object>> resultList = DbUtils.getResultList(result);
        Assertions.assertEquals(resultList.size(), 5);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.doe@example.com");
        Assertions.assertEquals(resultList.get(0), expectMap);

        result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", new ArrayList<>(), 5, 1);
        Assertions.assertEquals(result.get("totalPages"), 12);

        resultList = DbUtils.getResultList(result);
        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("id", 5);
        expectMap1.put("first_name", "Charlie");
        expectMap1.put("last_name", "Williams");
        expectMap1.put("email", "charlie.williams@example.com");
        expectMap1.put("age", 28);
        Assertions.assertEquals(resultList.get(0), expectMap1);

        result = DBManager.getDataWithPage(connection, dbType, "my_schema", "employees", new ArrayList<>(), 1, 12);
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
        columnNames.add("first_name");
        columnNames.add("fake_column");
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
        createSchema(connection, dbType);
        createFunc(connection, dbType);

        List<DatabaseFunction> result = DBManager.getFuncs(connection, dbType, "my_schema");
        Assertions.assertEquals(1, result.size());

        DatabaseFunction function = new DatabaseFunction("my_schema", "add_numbers");
        List<DatabaseFunction> expect = new ArrayList<>();
        expect.add(function);
        Assertions.assertEquals(expect, result);

        // more test cases
        Assertions.assertTrue(DBManager.getFuncs(connection, dbType, null).size() > 1);
        Assertions.assertTrue(DBManager.getFuncs(connection, dbType, "fakeSchema").isEmpty());
    }

    @Test
    void testExecuteFunction() throws SQLException {
        createSchema(connection, dbType);
        createFunc(connection, dbType);

        List<Map<String, Object>> result = DBManager.executeFunction(connection, dbType, "my_schema", "add_numbers", 10, 20);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("add_numbers", 30);
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
            DBManager.executeFunction(connection, dbType, "my_schema", "add_numbers", 10, "20");
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
        createSchema(connection, dbType);
        String sql = """
                CREATE TABLE my_schema.MyTable
                (
                  my_decimal   NUMERIC(10, 2),   -- Use NUMERIC in PostgreSQL instead of DECIMAL
                  my_date      DATE,             -- Date type
                  my_time      TIME,             -- Time type
                  my_timestamp TIMESTAMP,        -- Timestamp type
                  my_boolean   BOOLEAN,          -- Boolean type
                  my_blob      BYTEA             -- Use BYTEA for blob data in PostgreSQL
                );
                """;
        String insertData = """
                INSERT INTO my_schema.MyTable
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
                    1234.56,                       -- Numeric value
                    '2023-05-31',                  -- Date
                    '12:34:56',                    -- Time
                    '2023-05-31 12:34:56',         -- Timestamp
                    TRUE,                          -- Boolean value
                    E'\\\\x44424D61736B6572'         -- BYTEA data, inserting the binary representation of "DBMasker"
                );
                """;
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql);
        sqlList.add(insertData);
        DBManager.executeUpdateSQLBatch(connection, dbType, sqlList);

        List<Map<String, Object>> result = DBManager.getTableOrViewData(connection, dbType, "my_schema", "MyTable");
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

        Assertions.assertEquals(expectMap.get("my_decimal"), result.get(0).get("my_decimal"));
        Assertions.assertEquals(expectMap.get("my_date"), result.get(0).get("my_date"));
        Assertions.assertEquals(expectMap.get("my_time"), result.get(0).get("my_time"));
        Assertions.assertEquals(expectMap.get("my_timestamp"), result.get(0).get("my_timestamp"));
        Assertions.assertEquals(expectMap.get("my_boolean"), result.get(0).get("my_boolean"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("my_blob"), (byte[]) result.get(0).get("my_blob"));
    }

}
