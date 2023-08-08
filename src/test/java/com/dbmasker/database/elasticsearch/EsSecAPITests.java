package com.dbmasker.database.elasticsearch;

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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

class EsSecAPITests {

    private String driver = "org.elasticsearch.xpack.sql.jdbc.EsDriver";
    private String url;
    private String username;
    private String password;
    private String dbType = DbType.ELASTICSEARCH.getDbName();
    private String version = "v8";
    private String host = "192.168.3.165";
    private int port = 9200;

    private Connection connection;

    public void initConfig() {
        Properties properties = new Properties();
        try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("conf/elasticsearch.properties")) {
            properties.load(in);
            this.url = properties.getProperty("url");
            this.username = properties.getProperty("username");
            this.password = properties.getProperty("password");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    public void setUp() throws InterruptedException, SQLException, ClassNotFoundException {
        initConfig();
        connection = DBManager.createConnection(driver, url, username, password);

        // Run at first time
//        EsClient esClient = new EsClient(host, port, "http");
//        esClient.createData();
//        TimeUnit.SECONDS.sleep(1); // Wait for the data to be created
    }

    @AfterEach
    public void tearDown() throws SQLException {
        DBManager.closeConnection(connection);
        Config.getInstance().setDataSize(DBSecManager.MATCH_DATA_SIZE);
//        EsClient esClient = new EsClient(host, port, "http");
//        esClient.cleanData();
    }

    @Test
    void testGetTables() throws SQLException {
        List<String> tables = DBManager.getTables(connection, dbType, "");
        List<String> expectTables = new ArrayList<>();
        expectTables.add("employees");
        expectTables.add("library");

        Assertions.assertEquals(expectTables, tables);
    }

    @Test
    void testExecuteQuerySQL() throws SQLException {
        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees
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
        List<Map<String, Object>> expectResult = new ArrayList<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", Long.valueOf(1));
        expectMap.put("first_name", "John");
        expectMap.put("email", "[email redacted]");
        expectMap.put("age", Long.valueOf(30));
        expectResult.add(expectMap);

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
        expectMap1.put("id", Long.valueOf(1));
        expectMap1.put("first_name", "John");
        expectMap1.put("email", "john.doe@example.com");
        expectMap1.put("age", Long.valueOf(30));

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        List<Map<String, Object>> result2 = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(12, result2.size());
        Assertions.assertEquals(expectMap1, result2.get(0));

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("email", obfuscationRule);
        result2 = DBSecManager.execQuerySQLWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(12, result2.size());
        Assertions.assertEquals(expectMap1, result2.get(0));
    }

    @Test
    void testExecuteSQLScriptWithMask() throws SQLException {
        String sql = """
                SELECT id, first_name, last_name, email, age FROM employees;
                SELECT id, first_name, last_name, email, age FROM employees;
                """;

        ObfuscationRule obfuscationRule = new ObfuscationRule();
        obfuscationRule.setMethod(ObfuscationMethod.REPLACE);
        obfuscationRule.setRegex("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b");
        obfuscationRule.setReplacement("[email redacted]");
        Map<String, ObfuscationRule> obfuscationRuleMap = new HashMap<>();
        obfuscationRuleMap.put("email", obfuscationRule);

        List<List<Map<String, Object>>> result = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap);
        Assertions.assertEquals(result.size(), 2);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", Long.valueOf(1));
        expectMap.put("first_name", "John");
        expectMap.put("email", "[email redacted]");
        expectMap.put("age", Long.valueOf(30));

        Assertions.assertEquals(result.get(0), result.get(1));
        Assertions.assertEquals(result.get(0).size(), 12);
        Assertions.assertEquals(result.get(0).get(0), expectMap);
        Assertions.assertEquals(result.get(1).get(0), expectMap);

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
            //pass
        }

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("last_name", "Doe");
        expectMap2.put("id", Long.valueOf(1));
        expectMap2.put("first_name", "John");
        expectMap2.put("email", "john.doe@example.com");
        expectMap2.put("age", Long.valueOf(30));

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        List<List<Map<String, Object>>> result2 = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(result2.size(), 2);

        Assertions.assertEquals(result.get(0), result.get(1));
        Assertions.assertEquals(result2.get(1).size(), 12);
        Assertions.assertEquals(result2.get(0).get(0), expectMap2);
        Assertions.assertEquals(result2.get(1).get(0), expectMap2);


        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("email", obfuscationRule);
        result2 = DBSecManager.execSQLScriptWithMask(connection, dbType, sql, obfuscationRuleMap1);
        Assertions.assertEquals(result2.size(), 2);

        Assertions.assertEquals(result.get(0), result.get(1));
        Assertions.assertEquals(result2.get(1).size(), 12);
        Assertions.assertEquals(result2.get(0).get(0), expectMap2);
        Assertions.assertEquals(result2.get(1).get(0), expectMap2);
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
        obfuscationRuleMap.put("email", obfuscationRule);
        obfuscationRuleMap.put("age", obfuscationRule1);

        List<Map<String, Object>> result = DBSecManager.getDataWithMask(connection, dbType, "", "employees", obfuscationRuleMap);
        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("last_name", "Doe");
        expectMap.put("id", Long.valueOf(1));
        expectMap.put("first_name", "John");
        expectMap.put("email", "john.*@example.com");
        expectMap.put("age", "30-39");

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("last_name", "Smith");
        expectMap2.put("id", Long.valueOf(2));
        expectMap2.put("first_name", "Jane");
        expectMap2.put("email", "jane.sm*@example.com");
        expectMap2.put("age", "20-29");

        Assertions.assertEquals(expectMap, result.get(0));
        Assertions.assertEquals(expectMap2, result.get(1));

        // more test cases
        List<Map<String, Object>> result1 = DBSecManager.getDataWithMask(connection, dbType, null, "employees", obfuscationRuleMap);
        Assertions.assertEquals(result1, result);

        try {
            DBSecManager.getDataWithMask(connection, dbType, "fakeSchema", "employees", obfuscationRuleMap);
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
            DBSecManager.getDataWithMask(connection, dbType, "", "employees", null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        expectMap.remove("age");
        expectMap.put("age", Long.valueOf(30));
        expectMap.remove("email");
        expectMap.put("email", "john.doe@example.com");

        expectMap2.remove("age");
        expectMap2.put("age", Long.valueOf(28));
        expectMap2.remove("email");
        expectMap2.put("email", "jane.smith@example.com");


        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        result1 = DBSecManager.getDataWithMask(connection, dbType, "", "employees", obfuscationRuleMap1);
        Assertions.assertEquals(expectMap, result1.get(0));
        Assertions.assertEquals(expectMap2, result1.get(1));
        Assertions.assertEquals(12, result1.size());

        obfuscationRule.setReplacement(null);
        obfuscationRuleMap1.clear();
        obfuscationRuleMap1.put("email", obfuscationRule);
        result1 = DBSecManager.getDataWithMask(connection, dbType, "", "employees", obfuscationRuleMap1);
        Assertions.assertEquals(expectMap, result1.get(0));
        Assertions.assertEquals(expectMap2, result1.get(1));
        Assertions.assertEquals(12, result1.size());

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
        Map<String, Object> result = DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employees", columnNames,2, -2, obfuscationRuleMap);

        Assertions.assertEquals(result.get("totalPages"), 1);
        List<Map<String, Object>> resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 12);

        Map<String, Object> expectMap = new HashMap<>();
        expectMap.put("first_name", "Bob");
        expectMap.put("age", "30-39");
        expectMap.put("email", "bob.john*@example.com");
        Assertions.assertEquals(resultList.get(3), expectMap);

        columnNames = new ArrayList<>();
        columnNames.add("id");
        columnNames.add("age");

        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employees", columnNames,-12, 1, obfuscationRuleMap);

        Assertions.assertEquals(result.get("totalPages"), 1);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 12);

        Map<String, Object> expectMap1 = new HashMap<>();
        expectMap1.put("id", Long.valueOf(12));
        expectMap1.put("age", "30-39");
        Assertions.assertEquals(resultList.get(11), expectMap1);

        // more test cases
        try {
            DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employees", columnNames,-2, 3, null);
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            Assertions.assertEquals(e.getMessage(), ErrorMessages.NULL_OBFUSCATION_RULES_ERROR);
        }

        Map<String, ObfuscationRule> obfuscationRuleMap1 = new HashMap<>();
        obfuscationRuleMap1.put("fakeColumn", obfuscationRule);
        result = DBSecManager.getDataWithPageAndMask(connection, dbType, "", "employees", null,2, -3, obfuscationRuleMap1);

        Assertions.assertEquals(result.get("totalPages"), 1);
        resultList = DbUtils.getResultList(result);;
        Assertions.assertEquals(resultList.size(), 12);

        Map<String, Object> expectMap2 = new HashMap<>();
        expectMap2.put("id", Long.valueOf(6));
        expectMap2.put("last_name", "Brown");
        expectMap2.put("first_name", "David");
        expectMap2.put("age", Long.valueOf(42));
        expectMap2.put("email", "david.brown@example.com");
        Assertions.assertEquals(resultList.get(5), expectMap2);
    }

    @Test
    void testScanTableData() throws SQLException {
        String schemaName = "";

        String regex = "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b";
        List<String> regexList = new ArrayList<>();
        regexList.add(regex);

        List<SensitiveColumn> results = DBSecManager.scanTableData(connection, dbType, schemaName, "employees", regexList);
        Assertions.assertEquals(results.size(), 1);
        SensitiveColumn sensitiveColumn = new SensitiveColumn(schemaName, "employees", "email", regex);
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
        sensitiveColumn2.getMatchData().add(Long.valueOf(1));
        sensitiveColumn2.getMatchData().add(Long.valueOf(2));
        sensitiveColumn2.getMatchData().add(Long.valueOf(3));
        sensitiveColumn2.getMatchData().add(Long.valueOf(4));
        sensitiveColumn2.getMatchData().add(Long.valueOf(5));

        SensitiveColumn sensitiveColumn3 = new SensitiveColumn(schemaName, "employees", "age", regex2);
        sensitiveColumn3.getMatchData().add(Long.valueOf(30));
        sensitiveColumn3.getMatchData().add(Long.valueOf(28));
        sensitiveColumn3.getMatchData().add(Long.valueOf(30));
        sensitiveColumn3.getMatchData().add(Long.valueOf(35));
        sensitiveColumn3.getMatchData().add(Long.valueOf(28));
        expectResult2.add(sensitiveColumn3);

        expectResult2.add(sensitiveColumn2);

        Assertions.assertEquals(expectResult2, results2);

        Config.getInstance().setDataSize(1);
        List<SensitiveColumn> results3 = DBSecManager.scanTableData(connection, dbType, schemaName, "employees", regexList);
        List<SensitiveColumn> expectResult3 =  new ArrayList<>();
        SensitiveColumn sensitiveColumn31 = new SensitiveColumn(schemaName, "employees", "email", regex);
        sensitiveColumn31.getMatchData().add("john.doe@example.com");
        expectResult3.add(sensitiveColumn31);

        SensitiveColumn sensitiveColumn32 = new SensitiveColumn(schemaName, "employees", "id", regex2);
        sensitiveColumn32.getMatchData().add(Long.valueOf(1));

        SensitiveColumn sensitiveColumn33 = new SensitiveColumn(schemaName, "employees", "age", regex2);
        sensitiveColumn33.getMatchData().add(Long.valueOf(30));
        expectResult3.add(sensitiveColumn33);

        expectResult3.add(sensitiveColumn32);

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
    void testExecuteQuerySQLWithAlias() throws SQLException {
        String sql = """
                SELECT sub_query.first_name as fn, sub_query.fn as fn1, sub_query.ln, sub_query.em as e FROM (SELECT first_name, first_name as fn, last_name as ln, email as em FROM employees) as sub_query;
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
