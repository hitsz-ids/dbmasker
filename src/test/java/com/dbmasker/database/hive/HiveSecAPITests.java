package com.dbmasker.database.hive;

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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

class HiveSecAPITests {
    private String driver = "org.apache.hive.jdbc.HiveDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.HIVE.getDbName();
    private String version = "v2";

    private Connection connection;

    public void createTable(Connection connection, String dbType) throws SQLException {
        String sql = """
                  CREATE TABLE employees2 (
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

    public void createView(Connection connection, String dbType) throws SQLException {
        String sql = """
                CREATE VIEW employee_view2 AS
                SELECT id, first_name, last_name, email, age
                FROM employees2
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
    }

    public void insertData(Connection connection, String dbType) throws SQLException {
        String sql = """
                INSERT INTO employees2 VALUES (1, 'John', 'Doe', 'john.doe@example.com', 30)
                """;
        DBManager.executeUpdateSQL(connection, dbType, sql);
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

    public void clearData() throws SQLException {
        String sql = "DROP TABLE IF EXISTS employees2";
        DBManager.executeUpdateSQL(connection, dbType, sql);

        String sql1 = " DROP VIEW IF EXISTS employee_view2";
        DBManager.executeUpdateSQL(connection, dbType, sql1);
    }

    @AfterEach
    public void tearDown() throws SQLException {
        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees2
                """;

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
        obfuscationRule.setReplacement("[email redacted]");
        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("email", obfuscationRule);

        List<Map<String, Object>> result = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 12);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "[email redacted]");
        expectMap.put("age", 30);

        Assertions.assertEquals(result.get(0), expectMap);

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
        expectMap1.put("last_name", "Doe");
        expectMap1.put("id", 1);
        expectMap1.put("first_name", "John");
        expectMap1.put("email", "john.doe@example.com");
        expectMap1.put("age", 30);

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        List<Map<String, Object>> result2 = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(expectMap1, result2.get(0));

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("email", obfuscationRule);
        result2 = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(expectMap1, result2.get(0));


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

    }

    @Test
    void testExecuteSQLScriptWithMask() throws SQLException {
        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees2;
                SELECT id, first_name, last_name, email, age FROM employees2;
                """;

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
        obfuscationRule.setReplacement("[email redacted]");
        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("email", obfuscationRule);

        List<List<Map<String, Object>>> resultList = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap);
        Assertions.assertEquals(resultList.size(), 2);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", 1);
        expectMap.put("first_name", "John");
        expectMap.put("email", "[email redacted]");
        expectMap.put("age", 30);

        Assertions.assertEquals(resultList.get(0), resultList.get(1));
        Assertions.assertEquals(resultList.get(0).size(), 12);
        Assertions.assertEquals(resultList.get(0).get(0), expectMap);
        Assertions.assertEquals(resultList.get(1).get(0), expectMap);

        // more test cases
        String sqlScript = """
                UPDATE employees2 SET age = 33 WHERE id = 1;
                fakeSQL;
                """;
        try {
            DBSecManager.execSQLScriptWithMask(connection, dbType, sqlScript, obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("last_name", "Doe");
        expectMap2.put("id", 1);
        expectMap2.put("first_name", "John");
        expectMap2.put("email", "john.doe@example.com");
        expectMap2.put("age", 30);

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        List<List<Map<String, Object>>> resultList2 = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(resultList2.size(), 2);

        Assertions.assertEquals(resultList2.get(0), resultList2.get(1));
        Assertions.assertEquals(resultList2.get(1).size(), 12);
        Assertions.assertEquals(resultList2.get(0).get(0), expectMap2);
        Assertions.assertEquals(resultList2.get(1).get(0), expectMap2);


        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("email", obfuscationRule);
        resultList2 = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(resultList2.size(), 2);

        Assertions.assertEquals(resultList2.get(0), resultList2.get(1));
        Assertions.assertEquals(resultList2.get(1).size(), 12);
        Assertions.assertEquals(resultList2.get(0).get(0), expectMap2);
        Assertions.assertEquals(resultList2.get(1).get(0), expectMap2);
    }

    @Test
    void testSecGetTable() throws SQLException {
        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("(\\w*)\\w{3}@(\\w+)");
        obfuscationRule.setReplacement("$1*@$2");

        ObfuscationRule obfuscationRule1 = new ObfuscationRule();
        obfuscationRule1.setMethod(ObfuscationMethod.GENERALIZE);
        obfuscationRule1.setRange(10);

        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("employees2.email", obfuscationRule);
        obfuscationRuleMap.put("employees2.age", obfuscationRule1);

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "", "employees2", obfuscationRuleMap);
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("employees2.last_name", "Doe");
        expectMap.put("employees2.id", 1);
        expectMap.put("employees2.first_name", "John");
        expectMap.put("employees2.email", "john.*@example.com");
        expectMap.put("employees2.age", "30-39");

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("employees2.last_name", "Smith");
        expectMap2.put("employees2.id", 2);
        expectMap2.put("employees2.first_name", "Jane");
        expectMap2.put("employees2.email", "jane.sm*@example.com");
        expectMap2.put("employees2.age", "20-29");

        Assertions.assertEquals(expectMap, result.get(0));
        Assertions.assertEquals(expectMap2, result.get(1));

        Map<String, ObfuscationRule> obfuscationRuleMap2 = new HashMap<>();
        obfuscationRuleMap2.put("employee_view2.email", obfuscationRule);
        obfuscationRuleMap2.put("employee_view2.age", obfuscationRule1);

        Map<String, Object> expectViewMap = new HashMap<>();
        expectViewMap.put("employee_view2.last_name", "Doe");
        expectViewMap.put("employee_view2.id", 1);
        expectViewMap.put("employee_view2.first_name", "John");
        expectViewMap.put("employee_view2.email", "john.*@example.com");
        expectViewMap.put("employee_view2.age", "30-39");

        Map<String, Object> expectViewMap2 = new HashMap<>();
        expectViewMap2.put("employee_view2.last_name", "Smith");
        expectViewMap2.put("employee_view2.id", 2);
        expectViewMap2.put("employee_view2.first_name", "Jane");
        expectViewMap2.put("employee_view2.email", "jane.sm*@example.com");
        expectViewMap2.put("employee_view2.age", "20-29");


        List<Map<String, Object>> result2 = DBSecManager.getDataWithMask(connection, dbType, "", "employee_view2", obfuscationRuleMap2);
        Assertions.assertEquals(expectViewMap, result2.get(0));
        Assertions.assertEquals(expectViewMap2, result2.get(1));

        // more test cases
        Assertions.assertEquals(result2, DBSecManager.getDataWithMask(connection, dbType, null, "employee_view2", obfuscationRuleMap2));

        try {
            DBSecManager.getDataWithMask(connection, dbType, "fakeSchema", "employee_view2", obfuscationRuleMap);
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
            DBSecManager.getDataWithMask(connection, dbType, "", "employee_view2", null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        expectMap.remove("employees2.age");
        expectMap.put("employees2.age", 30);
        expectMap.remove("employees2.email");
        expectMap.put("employees2.email", "john.doe@example.com");

        expectMap2.remove("employees2.age");
        expectMap2.put("employees2.age", 28);
        expectMap2.remove("employees2.email");
        expectMap2.put("employees2.email", "jane.smith@example.com");

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        result2 = DBSecManager.getDataWithMask(connection, dbType, "", "employees2", obfuscationRuleMap1);
        Assertions.assertEquals(expectMap, result2.get(0));
        Assertions.assertEquals(expectMap2, result2.get(1));

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("email", obfuscationRule);
        result2 = DBSecManager.getDataWithMask(connection, dbType, "", "employees2", obfuscationRuleMap1);
        Assertions.assertEquals(expectMap, result2.get(0));
        Assertions.assertEquals(expectMap2, result2.get(1));
    }

    @Test
    void testSecGetTableWithPage() throws SQLException, ClassNotFoundException {
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
        Map<String, Object> result = DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employees2", columnNames,2, 3, obfuscationRuleMap);

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

        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employees2", columnNames,12, 1, obfuscationRuleMap);

        Assertions.assertEquals(result.get("totalPages"), 12);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 1);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("id", 12);
        expectMap1.put("age", "30-39");
        Assertions.assertEquals(resultList.get(0), expectMap1);

        // more test cases
        try {
            DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employee_view2", columnNames,2, 3, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employee_view2", null,2, 3, obfuscationRuleMap1);

        Assertions.assertEquals(result.get("totalPages"), 4);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 3);

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("employee_view2.id", 6);
        expectMap2.put("employee_view2.last_name", "Brown");
        expectMap2.put("employee_view2.first_name", "David");
        expectMap2.put("employee_view2.age", 42);
        expectMap2.put("employee_view2.email", "david.brown@example.com");
        Assertions.assertEquals(resultList.get(2), expectMap2);
    }

    @Test
    void testScanTableData() throws SQLException {
        String schemaName = "";

        String regex = "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b";
        List<String> regexList = new ArrayList<>();
        regexList.add(regex);

        List<SensitiveColumn> results = DBSecManager.scanTableData(connection, dbType, schemaName, "employees2", regexList);
        Assertions.assertEquals(results.size(), 1);
        SensitiveColumn sensitiveColumn = new SensitiveColumn(schemaName, "employees2", "employees2.email", regex);
        sensitiveColumn.getMatchData().add("john.doe@example.com");
        sensitiveColumn.getMatchData().add("jane.smith@example.com");
        sensitiveColumn.getMatchData().add("alice.smith@example.com");
        sensitiveColumn.getMatchData().add("bob.johnson@example.com");
        sensitiveColumn.getMatchData().add("charlie.williams@example.com");
        List<SensitiveColumn> expectResult = new ArrayList<>();
        expectResult.add(sensitiveColumn);

        Assertions.assertEquals(expectResult, results);

        String regex2 = "^[1-9][0-9]*$";
        regexList.add(regex2);

        List<SensitiveColumn> results2 = DBSecManager.scanTableData(connection, dbType, schemaName, "employees2", regexList);
        Assertions.assertEquals(results2.size(), 3);
        List<SensitiveColumn> expectResult2 =  new ArrayList<>();
        SensitiveColumn sensitiveColumn1 = new SensitiveColumn(schemaName, "employees2", "employees2.email", regex);
        sensitiveColumn1.getMatchData().add("john.doe@example.com");
        sensitiveColumn1.getMatchData().add("jane.smith@example.com");
        sensitiveColumn1.getMatchData().add("alice.smith@example.com");
        sensitiveColumn1.getMatchData().add("bob.johnson@example.com");
        sensitiveColumn1.getMatchData().add("charlie.williams@example.com");
        expectResult2.add(sensitiveColumn1);

        SensitiveColumn sensitiveColumn2 = new SensitiveColumn(schemaName, "employees2", "employees2.id", regex2);
        sensitiveColumn2.getMatchData().add(1);
        sensitiveColumn2.getMatchData().add(2);
        sensitiveColumn2.getMatchData().add(3);
        sensitiveColumn2.getMatchData().add(4);
        sensitiveColumn2.getMatchData().add(5);
        expectResult2.add(sensitiveColumn2);

        SensitiveColumn sensitiveColumn3 = new SensitiveColumn(schemaName, "employees2", "employees2.age", regex2);
        sensitiveColumn3.getMatchData().add(30);
        sensitiveColumn3.getMatchData().add(28);
        sensitiveColumn3.getMatchData().add(30);
        sensitiveColumn3.getMatchData().add(35);
        sensitiveColumn3.getMatchData().add(28);
        expectResult2.add(sensitiveColumn3);

        Assertions.assertEquals(expectResult2, results2);

        Config.getInstance().setDataSize(1);
        List<SensitiveColumn> results3 = DBSecManager.scanTableData(connection, dbType, schemaName, "employee_view2", regexList);
        List<SensitiveColumn> expectResult3 =  new ArrayList<>();
        SensitiveColumn sensitiveColumn31 = new SensitiveColumn(schemaName, "employee_view2", "employee_view2.email", regex);
        sensitiveColumn31.getMatchData().add("john.doe@example.com");
        expectResult3.add(sensitiveColumn31);

        SensitiveColumn sensitiveColumn32 = new SensitiveColumn(schemaName, "employee_view2", "employee_view2.id", regex2);
        sensitiveColumn32.getMatchData().add(1);
        expectResult3.add(sensitiveColumn32);

        SensitiveColumn sensitiveColumn33 = new SensitiveColumn(schemaName, "employee_view2", "employee_view2.age", regex2);
        sensitiveColumn33.getMatchData().add(30);
        expectResult3.add(sensitiveColumn33);

        Assertions.assertEquals(expectResult3, results3);

        // more test cases
        Assertions.assertEquals(DBSecManager.scanTableData(connection, dbType, null, "employees2", regexList).size(),3);

        try {
            DBSecManager.scanTableData(connection, dbType, schemaName, "employees2", null);
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
        results3 = DBSecManager.scanTableData(connection, dbType, schemaName, "employee_view2", regexList);
        Assertions.assertEquals(expectResult3, results3);
    }

    @Test
    void testMultiTypeData() throws SQLException {
        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.ADD_NOISE);
        obfuscationRule.setNoiseRange(Double.parseDouble("0.5"));

        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("my_table.my_decimal", obfuscationRule);

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "", "my_table", obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 1);
        Map<String, Object> expectMap = new HashMap<>();

        LocalDate localDate = LocalDate.parse("2023-05-31");
        ZoneId defaultZoneId = ZoneId.systemDefault();
        Instant instant = localDate.atStartOfDay(defaultZoneId).toInstant();
        Date date = Date.from(instant);
        expectMap.put("my_date", date);

        expectMap.put("my_timestamp", Timestamp.valueOf("2023-05-31 12:34:56"));
        expectMap.put("my_boolean", true);

        expectMap.put("my_blob", "DBMasker".getBytes());

        // check the added noise value
        Assertions.assertTrue(result.get(0).get("my_table.my_decimal") instanceof Double);
        // There's a very small chance that they would be equal
        Assertions.assertNotEquals(result.get(0).get("my_table.my_decimal") , Double.parseDouble("1234.56"));
        Assertions.assertTrue((Double)result.get(0).get("my_table.my_decimal") <= (Double.parseDouble("1234.56") + 0.25));
        Assertions.assertTrue((Double)result.get(0).get("my_table.my_decimal") >= (Double.parseDouble("1234.56") - 0.25));

        Assertions.assertEquals(expectMap.get("my_date"), result.get(0).get("my_table.my_date"));
        Assertions.assertEquals(expectMap.get("my_timestamp"), result.get(0).get("my_table.my_timestamp"));
        Assertions.assertEquals(expectMap.get("my_boolean"), result.get(0).get("my_table.my_boolean"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("my_blob"), (byte[]) result.get(0).get("my_table.my_blob"));
    }

    @Test
    void testExecuteQuerySQLWithAlias() throws SQLException, ClassNotFoundException {
        String sql = """
                SELECT first_name as fn, fn as fn1, ln, sub_query.em as e FROM (SELECT first_name, first_name as fn, last_name as ln, email as em FROM employees2) as sub_query;
                """;

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
        obfuscationRule.setReplacement("[email redacted]");
        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("email", obfuscationRule);

        List<Map<String, Object>> result = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap).get(0);
        Assertions.assertEquals(result.size(), 12);

        Map<String, Object> expectMap = new HashMap<>();
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("ln", "Doe");
        expectMap.put("fn", "John");
        expectMap.put("fn1", "John");
        expectMap.put("e", "[email redacted]");
        expectResult.add(expectMap);

        Assertions.assertEquals(result.get(0), expectResult.get(0));

        Config.getInstance().setHandleRename(false);

        result = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap).get(0);

        expectMap.put("e", "john.doe@example.com");

        Assertions.assertEquals(result.get(0), expectResult.get(0));

        Config.getInstance().setHandleRename(true);
    }
}
