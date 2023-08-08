package com.dbmasker.dialect;


import com.dbmasker.utils.DbUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * KingBaseDialect class implements the Dialect interface for KingBase Database.
 */
public class KingBaseDialect extends BaseDialect {

    /**
     * Default constructor for KingBaseDialect class.
     */
    public KingBaseDialect() {
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
        switch (type.toUpperCase()) {
            case "INT4", "INT2", "FLOAT4", "FLOAT8", "BOOL" -> {
                return String.valueOf(data);
            }
            case "BPCHAR", "VARCHAR", "TEXT", "INTERVAL" -> {
                return "'" + data.toString().replace("'", "''") + "'";
            }
            case "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "TIMETZ" -> {
                if (data instanceof Date date) {
                    DateFormat df;
                    switch (type.toUpperCase()) {
                        case "DATE" -> df = new SimpleDateFormat("yyyy-MM-dd");
                        case "TIME" -> df = new SimpleDateFormat("HH:mm:ss");
                        default -> df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }
                    return "'" + df.format(date) + "'";
                } else if (data instanceof String str) {
                    return "'" + (str).replace("'", "''") + "'";
                }
                return data.toString();
            }
            case "BYTEA" -> {
               return "E'\\\\x" + DbUtils.handBinaryData(data) + "'";
            }
            default -> {
                return data.toString();
            }
        }
    }
}
