package com.dbmasker.database.dameng;

import com.dbmasker.api.DBManager;
import com.dbmasker.api.DBSecManager;
import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.data.SensitiveColumn;
import com.dbmasker.utils.Config;
import com.dbmasker.database.DbType;
import com.dbmasker.utils.DbUtils;
import com.dbmasker.utils.ErrorMessages;
import com.dbmasker.utils.ObfuscationMethod;
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

class DMSecAPITests {
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
                SELECT id, first_name, last_name, email, age
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
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
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
        insertData(connection, dbType);

        String sql = """
                SELECT id, first_name, last_name, email, age FROM my_schema.employees;
                UPDATE my_schema.employees SET age = 31 WHERE id = 1;
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
        expectMap.put("AGE", 31);
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
            DBSecManager.getDataWithMask(connection, dbType, null, "employee_view", obfuscationRuleMap);
            Assertions.fail();
        } catch (SQLException e) {
            //pass
        }

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
        result2 = DBSecManager.getDataWithMask(connection, dbType, "my_schema", "employee_view", obfuscationRuleMap1);
        Assertions.assertEquals(expectResult, result2);

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("EMAIL", obfuscationRule);
        result2 = DBSecManager.getDataWithMask(connection, dbType, "my_schema", "employee_view", obfuscationRuleMap1);
        Assertions.assertEquals(expectResult, result2);
    }

    @Test
    void testSecGetTableWithPage() throws SQLException, ClassNotFoundException {
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
        obfuscationRuleMap1.put("FAKECOLUMN", obfuscationRule);
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
            DBSecManager.scanTableData(connection, dbType, null, "employees", regexList);
            Assertions.fail();
        } catch (SQLException e) {
            // pass
        }

        try {
            DBSecManager.scanTableData(connection, dbType, schemaName, "employees", null);
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
        results3 = DBSecManager.scanTableData(connection, dbType, schemaName, "employees", regexList);
        Assertions.assertEquals(results3.size(), 3);
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

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.ADD_NOISE);
        obfuscationRule.setNoiseRange(Double.parseDouble("0.5"));

        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("MY_DECIMAL", obfuscationRule);

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "my_schema", "my_table", obfuscationRuleMap);
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

        // check the added noise value
        Assertions.assertTrue(result.get(0).get("MY_DECIMAL") instanceof Double);
        // There's a very small chance that they would be equal
        Assertions.assertNotEquals(result.get(0).get("MY_DECIMAL") , Double.parseDouble("1234.56"));
        Assertions.assertTrue((Double)result.get(0).get("MY_DECIMAL") <= (Double.parseDouble("1234.56") + 0.25));
        Assertions.assertTrue((Double)result.get(0).get("MY_DECIMAL") >= (Double.parseDouble("1234.56") - 0.25));

        Assertions.assertEquals(expectMap.get("MY_DATE"), result.get(0).get("MY_DATE"));
        Assertions.assertEquals(expectMap.get("MY_TIME"), result.get(0).get("MY_TIME"));
        Assertions.assertEquals(expectMap.get("MY_TIMESTAMP"), result.get(0).get("MY_TIMESTAMP"));
        Assertions.assertArrayEquals((byte[]) expectMap.get("MY_BINARY"), (byte[]) result.get(0).get("MY_BINARY"));

        DmdbBlob blob = (DmdbBlob) result.get(0).get("MY_BLOB");
        Assertions.assertArrayEquals((byte[]) expectMap.get("MY_BLOB"), blob.getBytes(1, (int)blob.length()));
    }

    @Test
    void testExecuteQuerySQLWithAlias() throws SQLException{
        insertData(connection, dbType);

        String sql = """
                SELECT first_name as fn, fn as fn1, ln, sub_query.em as e FROM (SELECT first_name, first_name as fn, last_name as ln, email as em FROM my_schema.employees) as sub_query;
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
        expectMap.put("LN", "Doe");
        expectMap.put("FN", "John");
        expectMap.put("FN1", "John");
        expectMap.put("E", "[email redacted]");
        expectResult.add(expectMap);

        Assertions.assertEquals(result, expectResult);

        Config.getInstance().setHandleRename(false);

        result = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap);

        expectMap.put("E", "john.doe@example.com");

        Assertions.assertEquals(result, expectResult);

        Config.getInstance().setHandleRename(true);
    }
}
