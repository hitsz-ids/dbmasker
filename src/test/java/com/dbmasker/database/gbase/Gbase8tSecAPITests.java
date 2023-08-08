package com.dbmasker.database.gbase;

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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

class Gbase8tSecAPITests {
    private String driver = "com.informix.jdbc.IfxDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.GBASE8T.getDbName();
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
                SELECT id, first_name, last_name, email, age
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

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/gbase8t.properties")) {
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
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees;
                """;

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
        obfuscationRule.setReplacement("[email redacted]");
        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("email", obfuscationRule);

        List<Map<String, Object>> result = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 1);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "[email redacted]");
        expectMap.put("age", 30);
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
        expectMap1.put("last_name", "Doe");
        expectMap1.put("id", 1);
        expectMap1.put("first_name", "John");
        expectMap1.put("email", "john.doe@example.com");
        expectMap1.put("age", 30);
        expectResult1.add(expectMap1);

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        List<Map<String, Object>> result2 = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(expectResult1, result2);

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("email", obfuscationRule);
        result2 = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(expectResult1, result2);
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
            expectMap.put("age", 33);
            Assertions.assertEquals(result.get(0), expectResult);
        }

        Map<String, Object> expectMap2 = new HashMap<>();
        List<Map<String, Object>> expectResult2 = new ArrayList<>();
        expectMap2.put("last_name", "Doe");
        expectMap2.put("id", 1);
        expectMap2.put("first_name", "John");
        expectMap2.put("email", "john.doe@example.com");
        expectMap2.put("age", 33);
        expectResult2.add(expectMap2);

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        List<List<Map<String, Object>>> result2 = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(result2.size(), 3);
        Assertions.assertEquals(result2.get(0), expectResult2);
        Assertions.assertEquals(result2.get(1), expectResult1);
        expectMap2.put("age", 31);
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
    void testSecGetTable() throws SQLException {
        createTable(connection, dbType);
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
        obfuscationRuleMap.put("email", obfuscationRule);
        obfuscationRuleMap.put("age", obfuscationRule1);

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "", "employees", obfuscationRuleMap);
        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.*@example.com");
        expectMap.put("age", "30-39");

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("last_name", "Smith");
        expectMap2.put("id", 2);
        expectMap2.put("first_name", "Jane");
        expectMap2.put("email", "jane.sm*@example.com");
        expectMap2.put("age", "20-29");

        expectResult.add(expectMap);
        expectResult.add(expectMap2);

        Assertions.assertEquals(result, expectResult);

        createView(connection, dbType);
        List<Map<String, Object>> result2 = DBSecManager.getDataWithMask(connection, dbType, "", "employee_view", obfuscationRuleMap);
        Assertions.assertEquals(result2, expectResult);

        // more test cases
        Assertions.assertEquals(result2, DBSecManager.getDataWithMask(connection, dbType, null, "employee_view", obfuscationRuleMap));

        try {
            DBSecManager.getDataWithMask(connection, dbType, "fakeSchema", "employee_view", obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "", null, obfuscationRuleMap);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "", "fakeTable", obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "", "employee_view", null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        expectMap.remove("age");
        expectMap.put("age", 30);
        expectMap.remove("email");
        expectMap.put("email", "john.doe@example.com");

        expectMap2.remove("age");
        expectMap2.put("age", 28);
        expectMap2.remove("email");
        expectMap2.put("email", "jane.smith@example.com");

        expectResult.clear();
        expectResult.add(expectMap);
        expectResult.add(expectMap2);

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        result2 = DBSecManager.getDataWithMask(connection, dbType, "", "employee_view", obfuscationRuleMap1);
        Assertions.assertEquals(expectResult, result2);

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("email", obfuscationRule);
        result2 = DBSecManager.getDataWithMask(connection, dbType, "", "employee_view", obfuscationRuleMap1);
        Assertions.assertEquals(expectResult, result2);
    }

    @Test
    void testSecGetTableWithPage() throws SQLException, ClassNotFoundException {
        createTable(connection, dbType);
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
        obfuscationRuleMap.put("email", obfuscationRule);
        obfuscationRuleMap.put("age", obfuscationRule1);

        List<String> columnNames = new ArrayList<>();
        columnNames.add("first_name");
        columnNames.add("age");
        columnNames.add("email");
        Map<String, Object> result = DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employees", columnNames,2, 3, obfuscationRuleMap);

        Assertions.assertEquals(result.get("totalPages"), 4);
        List<Map<String, Object>> resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 3);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("first_name", "Bob");
        expectMap.put("age", "30-39");
        expectMap.put("email", "bob.john*@example.com");
        Assertions.assertEquals(resultList.get(0), expectMap);

        columnNames = new ArrayList<>();
        columnNames.add("id");
        columnNames.add("age");

        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employees", columnNames,12, 1, obfuscationRuleMap);

        Assertions.assertEquals(result.get("totalPages"), 12);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 1);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("id", 12);
        expectMap1.put("age", "30-39");
        Assertions.assertEquals(resultList.get(0), expectMap1);

        // more test cases
        try {
            DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employee_view", columnNames,2, 3, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employee_view", null,2, 3, obfuscationRuleMap1);

        Assertions.assertEquals(result.get("totalPages"), 4);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 3);

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("id", 6);
        expectMap2.put("last_name", "Brown");
        expectMap2.put("first_name", "David");
        expectMap2.put("age", 42);
        expectMap2.put("email", "david.brown@example.com");
        Assertions.assertEquals(resultList.get(2), expectMap2);
    }

    @Test
    void testScanTableData() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);
        insertData1(connection, dbType);

        String schemaName = "";

        String regex = "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b";
        List<String> regexList = new ArrayList<>();
        regexList.add(regex);

        List<SensitiveColumn> results = DBSecManager.scanTableData(connection, dbType, schemaName, "employees", regexList);
        Assertions.assertEquals(results.size(), 1);
        SensitiveColumn sensitiveColumn = new SensitiveColumn(schemaName, "employees", "email", regex);
        sensitiveColumn.getMatchData().add("john.doe@example.com");
        sensitiveColumn.getMatchData().add("jane.smith@example.com");
        List<SensitiveColumn> expectResult = new ArrayList<>();
        expectResult.add(sensitiveColumn);

        Assertions.assertEquals(expectResult, results);

        insertData2(connection, dbType);
        String regex2 = "^[1-9][0-9]*$";
        regexList.add(regex2);

        List<SensitiveColumn> results2 = DBSecManager.scanTableData(connection, dbType, schemaName, "employees", regexList);
        Assertions.assertEquals(results2.size(), 3);
        List<SensitiveColumn> expectResult2 =  new ArrayList<>();
        SensitiveColumn sensitiveColumn1 = new SensitiveColumn(schemaName, "employees", "email", regex);
        sensitiveColumn1.getMatchData().add("john.doe@example.com");
        sensitiveColumn1.getMatchData().add("jane.smith@example.com");
        sensitiveColumn1.getMatchData().add("alice.smith@example.com");
        sensitiveColumn1.getMatchData().add("bob.johnson@example.com");
        sensitiveColumn1.getMatchData().add("charlie.williams@example.com");
        expectResult2.add(sensitiveColumn1);

        SensitiveColumn sensitiveColumn2 = new SensitiveColumn(schemaName, "employees", "id", regex2);
        sensitiveColumn2.getMatchData().add(1);
        sensitiveColumn2.getMatchData().add(2);
        sensitiveColumn2.getMatchData().add(3);
        sensitiveColumn2.getMatchData().add(4);
        sensitiveColumn2.getMatchData().add(5);
        expectResult2.add(sensitiveColumn2);

        SensitiveColumn sensitiveColumn3 = new SensitiveColumn(schemaName, "employees", "age", regex2);
        sensitiveColumn3.getMatchData().add(30);
        sensitiveColumn3.getMatchData().add(28);
        sensitiveColumn3.getMatchData().add(30);
        sensitiveColumn3.getMatchData().add(35);
        sensitiveColumn3.getMatchData().add(28);
        expectResult2.add(sensitiveColumn3);

        Assertions.assertEquals(expectResult2, results2);

        Config.getInstance().setDataSize(1);
        createView(connection, dbType);
        List<SensitiveColumn> results3 = DBSecManager.scanTableData(connection, dbType, schemaName, "employee_view", regexList);
        List<SensitiveColumn> expectResult3 =  new ArrayList<>();
        SensitiveColumn sensitiveColumn31 = new SensitiveColumn(schemaName, "employee_view", "email", regex);
        sensitiveColumn31.getMatchData().add("john.doe@example.com");
        expectResult3.add(sensitiveColumn31);

        SensitiveColumn sensitiveColumn32 = new SensitiveColumn(schemaName, "employee_view", "id", regex2);
        sensitiveColumn32.getMatchData().add(1);
        expectResult3.add(sensitiveColumn32);

        SensitiveColumn sensitiveColumn33 = new SensitiveColumn(schemaName, "employee_view", "age", regex2);
        sensitiveColumn33.getMatchData().add(30);
        expectResult3.add(sensitiveColumn33);

        Assertions.assertEquals(expectResult3, results3);

        // more test cases
        Assertions.assertEquals(DBSecManager.scanTableData(connection, dbType, null, "employees", regexList).size(),3);

        try {
            DBSecManager.scanTableData(connection, dbType, "", "employees", null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_REGEX_LIST_ERROR);
        }

        try {
            DBSecManager.scanTableData(connection, dbType, "", null, regexList);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        String fakeRegex = "abc++";
        regexList.add(fakeRegex);
        results3 = DBSecManager.scanTableData(connection, dbType, "", "employees", regexList);
        Assertions.assertEquals(results3.size(), 3);
    }

    @Test
    void testMultiTypeData() throws SQLException, IOException {
        String sql = """
                CREATE TABLE my_table
                (
                  my_decimal   DECIMAL(10, 2),  -- DECIMAL type, total length is 10, decimal places are 2
                  my_date      DATE,            -- Date type
                  my_boolean   BOOLEAN         -- Boolean type, actually a TINYINT type in GBase 8a
                );
                """;
        String insertData = """
                INSERT INTO my_table
                (
                    my_decimal,
                    my_date,
                    my_boolean
                )
                VALUES
                (
                    1234.56,
                    '2023-05-31',
                     't'
                );
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

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "", "my_table", obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 1);
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("my_decimal", new BigDecimal("1234.56"));

        Date date = Date.valueOf("2023-05-31");
        expectMap.put("my_date", date);

        expectMap.put("my_boolean", true);

        // check the added noise value
        Assertions.assertTrue(result.get(0).get("my_decimal") instanceof Double);
        // There's a very small chance that they would be equal
        Assertions.assertNotEquals(result.get(0).get("my_decimal") , Double.parseDouble("1234.56"));
        Assertions.assertTrue((Double)result.get(0).get("my_decimal") <= (Double.parseDouble("1234.56") + 0.25));
        Assertions.assertTrue((Double)result.get(0).get("my_decimal") >= (Double.parseDouble("1234.56") - 0.25));

        Assertions.assertEquals(expectMap.get("my_date").toString(), result.get(0).get("my_date").toString());
        Assertions.assertEquals(expectMap.get("my_boolean"), result.get(0).get("my_boolean"));
    }

    @Test
    void testExecuteQuerySQLWithAlias() throws SQLException{
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT first_name as fn, fn as fn1, ln, sub_query.em as e FROM (SELECT first_name, first_name as fn, last_name as ln, email as em FROM employees) as sub_query;
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
        expectMap.put("ln", "Doe");
        expectMap.put("fn", "John");
        expectMap.put("fn1", "John");
        expectMap.put("e", "[email redacted]");
        expectResult.add(expectMap);

        Assertions.assertEquals(result, expectResult);

        Config.getInstance().setHandleRename(false);

        result = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap);

        expectMap.put("e", "john.doe@example.com");

        Assertions.assertEquals(result, expectResult);

        Config.getInstance().setHandleRename(true);
    }
}
