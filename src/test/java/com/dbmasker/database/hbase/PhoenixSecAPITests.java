package com.dbmasker.database.hbase;

import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.data.SensitiveColumn;
import com.dbmasker.utils.Config;
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

class PhoenixSecAPITests {
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
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees
                """;

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
        obfuscationRule.setReplacement("[email redacted]");
        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("EMAIL", obfuscationRule);

        List<Map<String, Object>> result = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", 1);
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "[email redacted]");
        expectMap.put("AGE", 30);
        expectResult.add(expectMap);

        Assertions.assertEquals(result, expectResult);

        // more test cases
        try {
            DBSecManager.execQuerySQLWithMask(connection, dbType, null, obfuscationRuleMap);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_SQL_ERROR);
        }

        try {
            DBSecManager.execQuerySQLWithMask(connection, dbType, sql, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        try {
            DBSecManager.execQuerySQLWithMask(connection, dbType, "fakeSQL", obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        Map<String, Object> expectMap1 = new HashMap<>();
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectMap1.put("LAST_NAME", "Doe");
        expectMap1.put("ID", 1);
        expectMap1.put("FIRST_NAME", "John");
        expectMap1.put("EMAIL", "john.doe@example.com");
        expectMap1.put("AGE", 30);
        expectResult1.add(expectMap1);

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        List<Map<String, Object>> result2 = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(expectResult1, result2);

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("EMAIL", obfuscationRule);
        result2 = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(expectResult1, result2);
    }

    @Test
    void testExecuteSQLScriptWithMask() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                upsert into my_schema.employees(id,age) values (1,31);
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
        obfuscationRule.setReplacement("[email redacted]");
        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("EMAIL", obfuscationRule);

        List<List<Map<String, Object>>> result = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 3);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", 1);
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "[email redacted]");
        expectMap.put("AGE", 30);
        expectResult.add(expectMap);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("rows", 1);
        List<Map<String, Object>> expectResult1 = new ArrayList<>();
        expectResult1.add(expectMap1);

        Assertions.assertEquals(result.get(0), expectResult);
        Assertions.assertEquals(result.get(1), expectResult1);
        Assertions.assertEquals(result.get(2), expectResult);

        expectMap.put("AGE", 31);
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
                UPDATE my_schema.employees SET age = 33 WHERE id = 1;
                fakeSQL;
                """;
        try {
            DBSecManager.execSQLScriptWithMask(connection, dbType, sqlScript, obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            sqlScript = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                """;
            result = DBSecManager.execSQLScriptWithMask(connection, dbType, sqlScript, obfuscationRuleMap);
            Assertions.assertEquals(result.get(0), expectResult);
        }

        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("LAST_NAME", "Doe");
        expectMap2.put("ID", 1);
        expectMap2.put("FIRST_NAME", "John");
        expectMap2.put("EMAIL", "john.doe@example.com");
        expectMap2.put("AGE", 31);
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
        obfuscationRuleMap1.put("EMAIL", obfuscationRule);
        result2 = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(result2.size(), 3);
        Assertions.assertEquals(result2.get(0), expectResult2);
        Assertions.assertEquals(result2.get(1), expectResult1);
        Assertions.assertEquals(result2.get(2), expectResult2);
    }

    @Test
    void testSecGetTable() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);
        insertData1(connection, dbType);

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("(\\w*)\\w{3}@(\\w+)");
        obfuscationRule.setReplacement("$1*@$2");

        ObfuscationRule obfuscationRule1 = new ObfuscationRule();
        obfuscationRule1.setMethod(ObfuscationMethod.GENERALIZE);
        obfuscationRule1.setRange(10);

        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("EMAIL", obfuscationRule);
        obfuscationRuleMap.put("AGE", obfuscationRule1);

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "MY_SCHEMA", "EMPLOYEES", obfuscationRuleMap);
        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", 1);
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.*@example.com");
        expectMap.put("AGE", "30-39");

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("LAST_NAME", "Smith");
        expectMap2.put("ID", 2);
        expectMap2.put("FIRST_NAME", "Jane");
        expectMap2.put("EMAIL", "jane.sm*@example.com");
        expectMap2.put("AGE", "20-29");

        expectResult.add(expectMap);
        expectResult.add(expectMap2);

        Assertions.assertEquals(result, expectResult);

        createView(connection, dbType);
        List<Map<String, Object>> result2 = DBSecManager.getDataWithMask(connection, dbType, "MY_SCHEMA", "EMPLOYEE_VIEW", obfuscationRuleMap);
        Assertions.assertEquals(result2, expectResult);

        // more test cases
        try {
            DBSecManager.getDataWithMask(connection, dbType, null, "EMPLOYEE_VIEW", obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "fakeSchema", "EMPLOYEE_VIEW", obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "MY_SCHEMA", null, obfuscationRuleMap);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "MY_SCHEMA", "fakeTable", obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "MY_SCHEMA", "EMPLOYEE_VIEW", null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        expectMap.remove("AGE");
        expectMap.put("AGE", 30);
        expectMap.remove("EMAIL");
        expectMap.put("EMAIL", "john.doe@example.com");

        expectMap2.remove("AGE");
        expectMap2.put("AGE", 28);
        expectMap2.remove("EMAIL");
        expectMap2.put("EMAIL", "jane.smith@example.com");

        expectResult.clear();
        expectResult.add(expectMap);
        expectResult.add(expectMap2);

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        result2 = DBSecManager.getDataWithMask(connection, dbType, "MY_SCHEMA", "EMPLOYEE_VIEW", obfuscationRuleMap1);
        Assertions.assertEquals(expectResult, result2);

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("EMAIL", obfuscationRule);
        result2 = DBSecManager.getDataWithMask(connection, dbType, "MY_SCHEMA", "EMPLOYEE_VIEW", obfuscationRuleMap1);
        Assertions.assertEquals(expectResult, result2);
    }

    @Test
    void testSecGetTableWithPage() throws SQLException, ClassNotFoundException {
        createSchema(connection, dbType);
        createView(connection, dbType);
        insertData(connection, dbType);
        insertData1(connection, dbType);
        insertData2(connection, dbType);

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("(\\w*)\\w{3}@(\\w+)");
        obfuscationRule.setReplacement("$1*@$2");

        ObfuscationRule obfuscationRule1 = new ObfuscationRule();
        obfuscationRule1.setMethod(ObfuscationMethod.GENERALIZE);
        obfuscationRule1.setRange(10);

        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("EMAIL", obfuscationRule);
        obfuscationRuleMap.put("AGE", obfuscationRule1);

        List<String> columnNames = new ArrayList<>();
        columnNames.add("FIRST_NAME");
        columnNames.add("AGE");
        columnNames.add("EMAIL");
        Map<String, Object> result = DBSecManager.getDataWithPageAndMask(connection, dbType, "my_schema", "employees", columnNames,2, 3, obfuscationRuleMap);

        Assertions.assertEquals(result.get("totalPages"), 4);
        List<Map<String, Object>> resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 3);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("FIRST_NAME", "Bob");
        expectMap.put("AGE", "30-39");
        expectMap.put("EMAIL", "bob.john*@example.com");
        Assertions.assertEquals(resultList.get(0), expectMap);

        columnNames = new ArrayList<>();
        columnNames.add("ID");
        columnNames.add("AGE");

        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "my_schema", "employees", columnNames,12, 1, obfuscationRuleMap);

        Assertions.assertEquals(result.get("totalPages"), 12);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 1);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("ID", 12);
        expectMap1.put("AGE", "30-39");
        Assertions.assertEquals(resultList.get(0), expectMap1);

        // more test cases
        try {
            DBSecManager.getDataWithPageAndMask(connection, dbType, "my_schema", "employee_view", columnNames,2, 3, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("FAKE_COLUMN", obfuscationRule);
        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "my_schema", "employee_view", null,2, 3, obfuscationRuleMap1);

        Assertions.assertEquals(result.get("totalPages"), 4);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 3);

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("ID", 6);
        expectMap2.put("LAST_NAME", "Brown");
        expectMap2.put("FIRST_NAME", "David");
        expectMap2.put("AGE", 42);
        expectMap2.put("EMAIL", "david.brown@example.com");
        Assertions.assertEquals(resultList.get(2), expectMap2);
    }

    @Test
    void testScanTableData() throws SQLException {
        createSchema(connection, dbType);
        insertData(connection, dbType);
        insertData1(connection, dbType);

        String schemaName = "MY_SCHEMA";

        String regex = "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b";
        List<String> regexList = new ArrayList<>();
        regexList.add(regex);

        List<SensitiveColumn> results = DBSecManager.scanTableData(connection, dbType, schemaName, "EMPLOYEES", regexList);
        Assertions.assertEquals(results.size(), 1);
        SensitiveColumn sensitiveColumn = new SensitiveColumn(schemaName, "EMPLOYEES", "EMAIL", regex);
        sensitiveColumn.getMatchData().add("john.doe@example.com");
        sensitiveColumn.getMatchData().add("jane.smith@example.com");
        List<SensitiveColumn> expectResult = new ArrayList<>();
        expectResult.add(sensitiveColumn);

        Assertions.assertEquals(expectResult, results);

        insertData2(connection, dbType);
        String regex2 = "^[1-9][0-9]*$";
        regexList.add(regex2);

        List<SensitiveColumn> results2 = DBSecManager.scanTableData(connection, dbType, schemaName, "EMPLOYEES", regexList);
        Assertions.assertEquals(results2.size(), 3);
        List<SensitiveColumn> expectResult2 =  new ArrayList<>();
        SensitiveColumn sensitiveColumn1 = new SensitiveColumn(schemaName, "EMPLOYEES", "EMAIL", regex);
        sensitiveColumn1.getMatchData().add("john.doe@example.com");
        sensitiveColumn1.getMatchData().add("jane.smith@example.com");
        sensitiveColumn1.getMatchData().add("alice.smith@example.com");
        sensitiveColumn1.getMatchData().add("bob.johnson@example.com");
        sensitiveColumn1.getMatchData().add("charlie.williams@example.com");
        expectResult2.add(sensitiveColumn1);

        SensitiveColumn sensitiveColumn2 = new SensitiveColumn(schemaName, "EMPLOYEES", "ID", regex2);
        sensitiveColumn2.getMatchData().add(1);
        sensitiveColumn2.getMatchData().add(2);
        sensitiveColumn2.getMatchData().add(3);
        sensitiveColumn2.getMatchData().add(4);
        sensitiveColumn2.getMatchData().add(5);
        expectResult2.add(sensitiveColumn2);

        SensitiveColumn sensitiveColumn3 = new SensitiveColumn(schemaName, "EMPLOYEES", "AGE", regex2);
        sensitiveColumn3.getMatchData().add(30);
        sensitiveColumn3.getMatchData().add(28);
        sensitiveColumn3.getMatchData().add(30);
        sensitiveColumn3.getMatchData().add(35);
        sensitiveColumn3.getMatchData().add(28);
        expectResult2.add(sensitiveColumn3);

        Assertions.assertEquals(expectResult2, results2);

        Config.getInstance().setDataSize(1);
        createView(connection, dbType);
        List<SensitiveColumn> results3 = DBSecManager.scanTableData(connection, dbType, schemaName, "EMPLOYEE_VIEW", regexList);
        List<SensitiveColumn> expectResult3 =  new ArrayList<>();
        SensitiveColumn sensitiveColumn31 = new SensitiveColumn(schemaName, "EMPLOYEE_VIEW", "EMAIL", regex);
        sensitiveColumn31.getMatchData().add("john.doe@example.com");
        expectResult3.add(sensitiveColumn31);

        SensitiveColumn sensitiveColumn32 = new SensitiveColumn(schemaName, "EMPLOYEE_VIEW", "ID", regex2);
        sensitiveColumn32.getMatchData().add(1);
        expectResult3.add(sensitiveColumn32);

        SensitiveColumn sensitiveColumn33 = new SensitiveColumn(schemaName, "EMPLOYEE_VIEW", "AGE", regex2);
        sensitiveColumn33.getMatchData().add(30);
        expectResult3.add(sensitiveColumn33);

        Assertions.assertEquals(expectResult3, results3);

        // more test cases
        try {
            DBSecManager.scanTableData(connection, dbType, null, "EMPLOYEES", regexList);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBSecManager.scanTableData(connection, dbType, schemaName, "EMPLOYEES", null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_REGEX_LIST_ERROR);
        }

        try {
            DBSecManager.scanTableData(connection, dbType, schemaName, null, regexList);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        String fakeRegex = "abc++";
        regexList.add(fakeRegex);
        results3 = DBSecManager.scanTableData(connection, dbType, schemaName, "EMPLOYEES", regexList);
        Assertions.assertEquals(results3.size(), 3);
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

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.ADD_NOISE);
        obfuscationRule.setNoiseRange(Double.parseDouble("0.5"));

        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("my_decimal", obfuscationRule);

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "my_schema", "my_table", obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 1);
        Map<String, Object> expectMap = new HashMap<>();

        LocalDate localDate = LocalDate.parse("2023-05-31");
        ZoneId defaultZoneId = ZoneId.systemDefault();
        Instant instant = localDate.atStartOfDay(defaultZoneId).toInstant();
        Date date = Date.from(instant);
        expectMap.put("my_date", date);

        expectMap.put("my_time", Time.valueOf("12:34:56"));
        expectMap.put("my_timestamp", Timestamp.valueOf("2023-05-31 12:34:56"));
        expectMap.put("my_boolean", true);

        expectMap.put("my_blob", "DBMasker".getBytes());

        // check the added noise value
        Assertions.assertTrue(result.get(0).get("MY_DECIMAL") instanceof Double);
        // There's a very small chance that they would be equal
        Assertions.assertNotEquals(result.get(0).get("MY_DECIMAL") , Double.parseDouble("1234.56"));
        Assertions.assertTrue((Double)result.get(0).get("MY_DECIMAL") <= (Double.parseDouble("1234.56") + 0.25));
        Assertions.assertTrue((Double)result.get(0).get("MY_DECIMAL") >= (Double.parseDouble("1234.56") - 0.25));

        Assertions.assertEquals(expectMap.get("my_date"), result.get(0).get("MY_DATE"));
        Assertions.assertEquals(expectMap.get("my_time"), result.get(0).get("MY_TIME"));
        Assertions.assertEquals(expectMap.get("my_timestamp"), result.get(0).get("MY_TIMESTAMP"));
        Assertions.assertEquals(expectMap.get("my_boolean"), result.get(0).get("MY_BOOLEAN"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("my_blob"), (byte[]) result.get(0).get("MY_BLOB"));
    }

    @Test
    void testExecuteQuerySQLWithAlias() throws SQLException{
        createSchema(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT first_name as fn, fn as fn1, ln as ln1, sub_query.em as e FROM (SELECT first_name, first_name as fn, last_name as ln, email as em FROM my_schema.employees) as sub_query;
                """;

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
        obfuscationRule.setReplacement("[email redacted]");
        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("email", obfuscationRule);

        List<Map<String, Object>> result = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap).get(0);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LN1", "Doe");
        expectMap.put("FN", "John");
        expectMap.put("FN1", "John");
        expectMap.put("E", "[email redacted]");
        expectResult.add(expectMap);

        Assertions.assertEquals(result, expectResult);

        Config.getInstance().setHandleRename(false);

        result = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap).get(0);

        expectMap.put("E", "john.doe@example.com");

        Assertions.assertEquals(result, expectResult);

        Config.getInstance().setHandleRename(true);
    }
}
