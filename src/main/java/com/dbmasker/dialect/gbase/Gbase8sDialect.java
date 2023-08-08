package com.dbmasker.dialect.gbase;


import com.dbmasker.dialect.BaseDialect;
import com.dbmasker.utils.DbUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Gbase8sDialect class implements the Dialect interface for Gbase 8s Database.
 */
public class Gbase8sDialect extends BaseDialect {

    /**
     * Default constructor for Gbase8sDialect class.
     */
    public Gbase8sDialect() {
        super();
    }

    /**
     * This method formats Java objects to be ready to use in SQL queries based on their SQL datatype.
     *
     * @param data The Java object data to be formatted.
     * @param type The SQL datatype of the column where the data will be used.
     * @return A string that is ready to be used in an SQL query.
     */
    @Override
    public String formatData(Object data, String type) {
        if (data == null) {
            return "NULL";
        }

        if (type == null) {
            return data.toString();
        }


        return switch (type.toUpperCase()) {
            case "BIGINT", "BIGSERIAL", "DECIMAL", "DOUBLE PRECISION", "FLOAT", "INT", "INT8", "INTEGER", "NUMERIC", "REAL", "SMALLINT", "SMALLFLOAT" -> String.valueOf(data);
            case "BOOLEAN" -> {
                if (data instanceof Boolean bool) {
                    yield Boolean.TRUE.equals(bool) ? "'t'" : "'f'";
                } else if (data instanceof Integer integer) {
                    yield integer == 0 ? "'f'" : "'t'";
                } else {
                    yield "'" + data + "'";
                }
            }
            case "CHAR", "CHARACTER", "CHARACTER VARYING", "NCHAR", "NVARCHAR", "VARCHAR" -> "'" + data.toString().replace("'", "''") + "'";
            case "DATE", "TIME", "TIMESTAMP", "DATETIME" -> DbUtils.handDateData(data);
            case "SET" -> transformToSet(data.toString(), "SET");
            case "LIST", "LVARCHAR" -> transformToSet(data.toString(), "LIST");
            default -> data.toString();
        };
    }

    /**
     * This method transforms a string representation of a list (e.g., "[1, 2, 3]") into a
     * string representation of a database set or list (e.g., "SET{'1', '2', '3'}").
     *
     * @param input The string to be transformed.
     * @param type The database data structure type, such as "SET", "LIST", etc.
     * @return A string that represents the input in a database-friendly format, or the original input
     *         if it does not match the list pattern.
     */
    public static String transformToSet(String input, String type) {
        String output = input;
        Pattern pattern = Pattern.compile("\\[(.*)\\]");  // Regular expression to match contents within brackets
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            String[] elements = matcher.group(1).split(",");
            StringBuilder transformed = new StringBuilder(type + "{");  // Start building the transformed string
            for (int i = 0; i < elements.length; i++) {
                transformed.append("'").append(elements[i].trim()).append("'");  // Add the element to the transformed string, surrounded by single quotes
                if (i < elements.length - 1) {  // If it's not the last element
                    transformed.append(",");
                }
            }
            transformed.append("}");
            output = transformed.toString();  // Update the output to the transformed string
        }

        return output;
    }


}
