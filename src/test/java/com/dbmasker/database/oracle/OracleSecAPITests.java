package com.dbmasker.database.oracle;

import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.data.SensitiveColumn;
import com.dbmasker.utils.Config;
import com.dbmasker.database.DbType;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import com.dbmasker.utils.ObfuscationMethod;
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
import java.sql.Timestamp;
import java.time.*;
import java.util.*;

class OracleSecAPITests {

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

    public void createView(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE VIEW employee_view AS
                SELECT id, first_name, last_name, email, age
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

        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees
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
        expectMap.put("ID", new BigDecimal(1));
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "[email redacted]");
        expectMap.put("AGE", new BigDecimal(30));
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
        expectMap1.put("ID", new BigDecimal(1));
        expectMap1.put("FIRST_NAME", "John");
        expectMap1.put("EMAIL", "john.doe@example.com");
        expectMap1.put("AGE", new BigDecimal(30));
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
    void testExecuteSQLScriptWithMask() throws SQLException, ClassNotFoundException {
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
        obfuscationRuleMap.put("EMAIL", obfuscationRule);

        List<List<Map<String, Object>>> result = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 3);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", new BigDecimal(1));
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "[email redacted]");
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
        expectMap2.put("LAST_NAME", "Doe");
        expectMap2.put("ID", new BigDecimal(1));
        expectMap2.put("FIRST_NAME", "John");
        expectMap2.put("EMAIL", "john.doe@example.com");
        expectMap2.put("AGE", new BigDecimal(31));
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
        obfuscationRuleMap.put("EMAIL", obfuscationRule);
        obfuscationRuleMap.put("AGE", obfuscationRule1);

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "M", "EMPLOYEES", obfuscationRuleMap);
        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("LAST_NAME", "Doe");
        expectMap.put("ID", new BigDecimal(1));
        expectMap.put("FIRST_NAME", "John");
        expectMap.put("EMAIL", "john.*@example.com");
        expectMap.put("AGE", "30-39");

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("LAST_NAME", "Smith");
        expectMap2.put("ID", new BigDecimal(2));
        expectMap2.put("FIRST_NAME", "Jane");
        expectMap2.put("EMAIL", "jane.sm*@example.com");
        expectMap2.put("AGE", "20-29");

        expectResult.add(expectMap);
        expectResult.add(expectMap2);

        Assertions.assertEquals(result, expectResult);

        createView(connection, dbType);
        List<Map<String, Object>> result2 = DBSecManager.getDataWithMask(connection, dbType, "M", "EMPLOYEE_VIEW", obfuscationRuleMap);
        Assertions.assertEquals(result2, expectResult);

        // more test cases
        Assertions.assertEquals(expectResult, DBSecManager.getDataWithMask(connection, dbType, null, "EMPLOYEE_VIEW", obfuscationRuleMap));

        try {
            DBSecManager.getDataWithMask(connection, dbType, "fakeSchema", "employee_view", obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "my_schema", null, obfuscationRuleMap);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_TABLE_OR_VIEW_NAME_ERROR);
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "my_schema", "fakeTable", obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        try {
            DBSecManager.getDataWithMask(connection, dbType, "my_schema", "employee_view", null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        expectMap.remove("AGE");
        expectMap.put("AGE", new BigDecimal(30));
        expectMap.remove("EMAIL");
        expectMap.put("EMAIL", "john.doe@example.com");

        expectMap2.remove("AGE");
        expectMap2.put("AGE", new BigDecimal(28));
        expectMap2.remove("EMAIL");
        expectMap2.put("EMAIL", "jane.smith@example.com");

        expectResult.clear();
        expectResult.add(expectMap);
        expectResult.add(expectMap2);

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        result2 = DBSecManager.getDataWithMask(connection, dbType, "M", "EMPLOYEE_VIEW", obfuscationRuleMap1);
        Assertions.assertEquals(expectResult, result2);

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("EMAIL", obfuscationRule);
        result2 = DBSecManager.getDataWithMask(connection, dbType, "M", "EMPLOYEE_VIEW", obfuscationRuleMap1);
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
        obfuscationRuleMap.put("EMAIL", obfuscationRule);
        obfuscationRuleMap.put("AGE", obfuscationRule1);

        List<String> columnNames = new ArrayList<>();
        columnNames.add("FIRST_NAME");
        columnNames.add("AGE");
        columnNames.add("EMAIL");
        Map<String, Object> result = DBSecManager.getDataWithPageAndMask(connection, dbType, "M", "EMPLOYEES", columnNames,2, 3, obfuscationRuleMap);

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

        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "M", "EMPLOYEES", columnNames,12, 1, obfuscationRuleMap);

        Assertions.assertEquals(result.get("totalPages"), 12);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 1);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("ID", new BigDecimal(12));
        expectMap1.put("AGE", "30-39");
        Assertions.assertEquals(resultList.get(0), expectMap1);

        // more test cases
        try {
            DBSecManager.getDataWithPageAndMask(connection, dbType, "M", "EMPLOYEE_VIEW", columnNames,2, 3, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("FAKECOLUMN", obfuscationRule);
        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "M", "EMPLOYEE_VIEW", null,2, 3, obfuscationRuleMap1);

        Assertions.assertEquals(result.get("totalPages"), 4);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 3);

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("ID", new BigDecimal(6));
        expectMap2.put("LAST_NAME", "Brown");
        expectMap2.put("FIRST_NAME", "David");
        expectMap2.put("AGE", new BigDecimal(42));
        expectMap2.put("EMAIL", "david.brown@example.com");
        Assertions.assertEquals(resultList.get(2), expectMap2);
    }

    @Test
    void testScanTableData() throws SQLException {
        createTable(connection, dbType);
        insertData(connection, dbType);
        insertData1(connection, dbType);

        String schemaName = "M";

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
        sensitiveColumn2.getMatchData().add(new BigDecimal(1));
        sensitiveColumn2.getMatchData().add(new BigDecimal(2));
        sensitiveColumn2.getMatchData().add(new BigDecimal(3));
        sensitiveColumn2.getMatchData().add(new BigDecimal(4));
        sensitiveColumn2.getMatchData().add(new BigDecimal(5));
        expectResult2.add(sensitiveColumn2);

        SensitiveColumn sensitiveColumn3 = new SensitiveColumn(schemaName, "EMPLOYEES", "AGE", regex2);
        sensitiveColumn3.getMatchData().add(new BigDecimal(30));
        sensitiveColumn3.getMatchData().add(new BigDecimal(28));
        sensitiveColumn3.getMatchData().add(new BigDecimal(30));
        sensitiveColumn3.getMatchData().add(new BigDecimal(35));
        sensitiveColumn3.getMatchData().add(new BigDecimal(28));
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
        sensitiveColumn32.getMatchData().add(new BigDecimal(1));
        expectResult3.add(sensitiveColumn32);

        SensitiveColumn sensitiveColumn33 = new SensitiveColumn(schemaName, "EMPLOYEE_VIEW", "AGE", regex2);
        sensitiveColumn33.getMatchData().add(new BigDecimal(30));
        expectResult3.add(sensitiveColumn33);

        Assertions.assertEquals(expectResult3, results3);



        // more test cases
        List<SensitiveColumn> results4 = DBSecManager.scanTableData(connection, dbType, null, "EMPLOYEE_VIEW", regexList);
        Assertions.assertEquals(3, results4.size());

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

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.ADD_NOISE);
        obfuscationRule.setNoiseRange(Double.parseDouble("0.5"));

        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("MY_DECIMAL", obfuscationRule);

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "M", "MyTable", obfuscationRuleMap);
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

        // check the added noise value
        Assertions.assertTrue(result.get(0).get("MY_DECIMAL") instanceof Double);
        // There's a very small chance that they would be equal
        Assertions.assertNotEquals(result.get(0).get("MY_DECIMAL") , Double.parseDouble("1234.56"));
        Assertions.assertTrue((Double)result.get(0).get("MY_DECIMAL") <= (Double.parseDouble("1234.56") + 0.25));
        Assertions.assertTrue((Double)result.get(0).get("MY_DECIMAL") >= (Double.parseDouble("1234.56") - 0.25));

        Assertions.assertEquals(expectMap.get("MY_DATE"), result.get(0).get("MY_DATE"));
        Assertions.assertEquals(expectMap.get("MY_TIME"), result.get(0).get("MY_TIME"));
        Assertions.assertEquals(expectMap.get("MY_TIME_STAMP"), result.get(0).get("MY_TIME_STAMP"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("MY_BINARY"), (byte[]) result.get(0).get("MY_BINARY"));
    }

    @Test
    void testExecuteQuerySQLWithAlias() throws SQLException{
        createTable(connection, dbType);
        insertData(connection, dbType);

        String sql = """
                SELECT sub_query.first_name "fn", first_name "fn1", last_name "ln", sub_query.email "e"
                FROM (
                    SELECT first_name, last_name, email
                    FROM employees
                ) sub_query
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
