package com.dbmasker.utils;

import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.*;

class DbUtilsTest {

    @Test
    void testGetAndMatchColumnRename() {
        // Positive case
        Map<String, Set<String>> renameMap = DbUtils.getColumnRename("SELECT column1 as alias1, column2 as alias2 FROM table");
        Assertions.assertEquals(2, renameMap.size());
        Assertions.assertEquals("column1", renameMap.get("alias1").iterator().next());
        Assertions.assertEquals("column2", renameMap.get("alias2").iterator().next());
        Assertions.assertTrue(DbUtils.columnMatch("alias1", "column1", renameMap));
        Assertions.assertTrue(DbUtils.columnMatch("alias2", "column2", renameMap));
        Assertions.assertFalse(DbUtils.columnMatch("alias2", "column1", renameMap));

        // Negative case - no alias
        renameMap = DbUtils.getColumnRename("SELECT column1, column2 FROM table");
        Assertions.assertTrue(renameMap.isEmpty());

        // Boundary case - alias is the same as the original column name
        renameMap = DbUtils.getColumnRename("SELECT column1 as column1, column2 as column2 FROM table");
        Assertions.assertEquals(0, renameMap.size());
        Assertions.assertTrue(DbUtils.columnMatch("column1", "column1", renameMap));

        // Boundary case - alias contains special characters
        renameMap = DbUtils.getColumnRename("SELECT column1 as `alias#1`, column2 as `alias#2` FROM table");
        Assertions.assertEquals(2, renameMap.size());
        Assertions.assertEquals("column1", renameMap.get("`alias#1`").iterator().next());
        Assertions.assertEquals("column2", renameMap.get("`alias#2`").iterator().next());
        Assertions.assertTrue(DbUtils.columnMatch("`alias#2`", "column2", renameMap));

        // Positive case - FROM clause contains a sub-query with an alias
        renameMap = DbUtils.getColumnRename("SELECT column1 as alias1, column2 as alias2 FROM (SELECT column1, column2 FROM table) as sub_query");
        Assertions.assertEquals(2, renameMap.size());
        Assertions.assertEquals("column1", renameMap.get("alias1").iterator().next());
        Assertions.assertEquals("column2", renameMap.get("alias2").iterator().next());
        Assertions.assertTrue(DbUtils.columnMatch("alias2", "column2", renameMap));

        // Positive case - Sub-query and main query select items both have aliases
        renameMap = DbUtils.getColumnRename("SELECT sub_alias1 as main_alias1, sub_alias2 as main_alias2 FROM (SELECT column1 as sub_alias1, column2 as sub_alias2 FROM table) as sub_query");
        Assertions.assertEquals(4, renameMap.size());
        Assertions.assertTrue(renameMap.get("main_alias1").contains("sub_alias1"));
        Assertions.assertTrue(renameMap.get("main_alias2").contains("sub_alias2"));
        Assertions.assertEquals("column1", renameMap.get("sub_alias1").iterator().next());
        Assertions.assertEquals("column2", renameMap.get("sub_alias2").iterator().next());
        Assertions.assertTrue(DbUtils.columnMatch("main_alias1", "sub_alias1", renameMap));
        Assertions.assertTrue(DbUtils.columnMatch("main_alias1", "column1", renameMap));
        Assertions.assertTrue(DbUtils.columnMatch("main_alias2", "column2", renameMap));
    }

    @Test
    void testGetColumnRename2() {
        Map<String, Set<String>> renameMap = DbUtils.getColumnRename("SELECT first_name as fn, fn as fn1, ln as fn FROM (SELECT first_name, first_name as fn, last_name as ln FROM employees);");
        Assertions.assertEquals(3, renameMap.size());

        Assertions.assertEquals(new HashSet<>(Set.of("first_name", "ln", "last_name")), renameMap.get("fn"));
        Assertions.assertEquals(new HashSet<>(Set.of("first_name", "ln", "last_name", "fn")), renameMap.get("fn1"));
        Assertions.assertEquals(new HashSet<>(Set.of("last_name")), renameMap.get("ln"));
        Assertions.assertTrue(DbUtils.columnMatch("fn", "first_name", renameMap));
        Assertions.assertTrue(DbUtils.columnMatch("fn", "last_name", renameMap));
        Assertions.assertTrue(DbUtils.columnMatch("fn", "ln", renameMap));
        Assertions.assertTrue(DbUtils.columnMatch("fn", "fn", renameMap));
        Assertions.assertFalse(DbUtils.columnMatch("fn", "fn1", renameMap));

        renameMap = DbUtils.getColumnRename("SELECT fn1 as fn, fn as fn1, ln as fn FROM (SELECT first_name, first_name as fn1, first_name as fn, last_name as ln FROM employees);");
        Assertions.assertEquals(3, renameMap.size());

        Assertions.assertEquals(new HashSet<>(Set.of("first_name", "ln", "last_name", "fn1")), renameMap.get("fn"));
        Assertions.assertEquals(new HashSet<>(Set.of("first_name", "ln", "last_name", "fn")), renameMap.get("fn1"));
        Assertions.assertEquals(new HashSet<>(Set.of("last_name")), renameMap.get("ln"));
        Assertions.assertTrue(DbUtils.columnMatch("fn", "first_name", renameMap));
        Assertions.assertTrue(DbUtils.columnMatch("fn", "last_name", renameMap));
        Assertions.assertTrue(DbUtils.columnMatch("fn", "ln", renameMap));
        Assertions.assertFalse(DbUtils.columnMatch("ln", "first_name", renameMap));
        Assertions.assertTrue(DbUtils.columnMatch("fn1", "last_name", renameMap));

        String sql = """
                SELECT sub_query.first_name "fn", sub_query.first_name "fn1", sub_query.last_name "ln", sub_query.email "e"
                FROM (
                SELECT first_name, last_name, email
                FROM employees
                ) sub_query
                """;
        renameMap = DbUtils.getColumnRename(sql);
        Assertions.assertEquals(4, renameMap.size());
        Assertions.assertEquals(new HashSet<>(Set.of("first_name")), renameMap.get("fn"));
        Assertions.assertEquals(new HashSet<>(Set.of("first_name")), renameMap.get("fn1"));
        Assertions.assertEquals(new HashSet<>(Set.of("email")), renameMap.get("e"));
        Assertions.assertEquals(new HashSet<>(Set.of("last_name")), renameMap.get("ln"));
    }

    @Test
    void testTransform() {
        Map<String, Set<String>> originalMap = new HashMap<>();
        originalMap.put("a", new HashSet<>(Set.of("b", "c", "d")));
        originalMap.put("b", new HashSet<>(Set.of("e", "f")));
        originalMap.put("c", new HashSet<>(Set.of("g")));
        originalMap.put("e", new HashSet<>(Set.of("h")));

        Map<String, Set<String>> result = DbUtils.transform(originalMap);

        Map<String, Set<String>> expectedMap = new HashMap<>();
        expectedMap.put("a", new HashSet<>(Set.of("b", "c", "d", "e", "f", "g", "h")));
        expectedMap.put("b", new HashSet<>(Set.of("e", "f", "h")));
        expectedMap.put("c", new HashSet<>(Set.of("g")));
        expectedMap.put("e", new HashSet<>(Set.of("h")));

        Assertions.assertEquals(expectedMap, result);

        originalMap = new HashMap<>();
        originalMap.put("a", new HashSet<>(Set.of("b", "c", "d")));
        originalMap.put("b", new HashSet<>(Set.of("e", "f")));

        result = DbUtils.transform(originalMap);

        Map<String, Set<String>> wrongMap = new HashMap<>();
        wrongMap.put("a", new HashSet<>(Set.of("b", "c", "d", "e", "f", "g", "h")));  // wrong expected result
        wrongMap.put("b", new HashSet<>(Set.of("e", "f", "h")));  // wrong expected result

        Assertions.assertNotEquals(wrongMap, result);
    }

    @Test
    void testSplitSqlScript() throws JSQLParserException {
        String sql = "SELECT * FROM table1; SELECT * FROM table2;";
        List<String> sqlList = DbUtils.splitSqlScript(sql);

        Assertions.assertEquals(2, sqlList.size());
        Assertions.assertEquals("SELECT * FROM table1", sqlList.get(0));
        Assertions.assertEquals("SELECT * FROM table2", sqlList.get(1));

        sql = "INSERT INTO users VALUES ('john;doe'); SELECT * FROM users;";
        sqlList = DbUtils.splitSqlScript(sql);

        Assertions.assertEquals(2, sqlList.size());
        Assertions.assertEquals("INSERT INTO users VALUES ('john;doe')", sqlList.get(0));
        Assertions.assertEquals("SELECT * FROM users", sqlList.get(1));

        sql = """
              SELECT * FROM users; -- This is a comment;
              INSERT INTO users VALUES ('john', 'doe');
              """;
        sqlList = DbUtils.splitSqlScript(sql);

        Assertions.assertEquals(2, sqlList.size());
        Assertions.assertEquals("SELECT * FROM users", sqlList.get(0));
        Assertions.assertEquals("INSERT INTO users VALUES ('john', 'doe')", sqlList.get(1));

        sql = """
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
        sqlList = DbUtils.splitSqlScript(sql);

        Assertions.assertEquals(1, sqlList.size());
        Assertions.assertEquals("CREATE OR REPLACE FUNCTION my_schema . add_numbers ( a INTEGER , b INTEGER ) RETURNS INTEGER AS $$ DECLARE s INTEGER; BEGIN s := a + b; RETURN s; END; $$ LANGUAGE plpgsql;", sqlList.get(0));
    }
}
