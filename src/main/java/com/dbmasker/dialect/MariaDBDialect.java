package com.dbmasker.dialect;


import com.dbmasker.utils.DbUtils;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * MariaDBDialect class implements the Dialect interface for MariaDB.
 */
public class MariaDBDialect extends BaseDialect {

    /**
     * Default constructor for MariaDBDialect class.
     */
    public MariaDBDialect() {
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

        String typeUpper = type.toUpperCase();
        if (typeUpper.equals("NULL")) {
            return "NULL";
        }
        if (typeUpper.startsWith("CHAR") || typeUpper.startsWith("VARCHAR")
                || typeUpper.equals("TEXT") || typeUpper.equals("TINYTEXT")
                || typeUpper.equals("MEDIUMTEXT") || typeUpper.equals("LONGTEXT")) {
            String text = data.toString();
            text = text.replace("'", "''");  // Replace single quotes with two single quotes.
            return "'" + text + "'";
        }
        if (typeUpper.equals("BOOLEAN")) {
            if (data instanceof Boolean boolData) {
                return Boolean.toString(boolData);
            } else {
                return data.toString();
            }
        }
        if (typeUpper.startsWith("TIME") || typeUpper.startsWith("DATE")
                || typeUpper.startsWith("TIMESTAMP")) {
            return handTimeData(data);
        }
        if (typeUpper.equals("YEAR")) {
            return "'" + data + "'";
        }
        if (typeUpper.equals("BINARY") || typeUpper.equals("VARBINARY")
                || typeUpper.equals("TINYBLOB") || typeUpper.equals("BLOB")
                || typeUpper.equals("MEDIUMBLOB") || typeUpper.equals("LONGBLOB")) {
            String binaryData = DbUtils.handBinaryData(data);
            return "x'" + binaryData + "'";
        }
        if (typeUpper.equals("BIT") || typeUpper.equals("TINYINT") || typeUpper.equals("SMALLINT")
                || typeUpper.equals("MEDIUMINT") || typeUpper.equals("INT") || typeUpper.equals("BIGINT")
                || typeUpper.equals("DECIMAL") || typeUpper.equals("FLOAT") || typeUpper.equals("DOUBLE")) {
            return data.toString();
        }
        if (typeUpper.equals("ENUM") || typeUpper.equals("SET")) {
            String text = data.toString();
            text = text.replace("'", "''");  // Replace single quotes with two single quotes.
            return "'" + text + "'";
        }
        // For other types, use the default handling
        return data.toString();
    }

    /**
     * This method formats Java Date or Timestamp objects to be ready to use in SQL queries.
     * It checks if the object is a Date or a Timestamp and formats it accordingly.
     * If it is neither, it directly converts the object to string.
     *
     * @param data The Java object data to be formatted.
     * @return A string that is formatted for use in an SQL query in the case of DATE, TIME, and TIMESTAMP types.
     */
    private String handTimeData(Object data) {
        String text = "";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (data instanceof Date date) {
            text = sdf.format(date);
        } else if (data instanceof Timestamp ts) {
            text = sdf.format(new Date(ts.getTime()));
        } else {
            // If data is not a Date or Timestamp, use it directly.
            text = data.toString();
        }
        text = text.replace("'", "''");  // Replace single quotes with two single quotes.
        return "'" + text + "'";
    }
}
