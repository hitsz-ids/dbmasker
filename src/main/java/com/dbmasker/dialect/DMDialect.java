package com.dbmasker.dialect;


import com.dbmasker.utils.DbUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * DMDialect class implements the Dialect interface for Dameng Database.
 */
public class DMDialect extends BaseDialect {

    /**
     * Default constructor for DMDialect class.
     */
    public DMDialect() {
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
        SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");

        if (data == null) {
            return "NULL";
        }

        if (type == null) {
            return data.toString();
        }

        return switch (type.toUpperCase()) {
            case "INT", "TINYINT", "SMALLINT", "INTEGER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE" -> String.valueOf(data);
            case "DATE" -> "DATE '" + ((data instanceof Date date) ? dateFormatter.format(date) : data.toString()) + "'";
            case "TIME" -> "TIME '" + ((data instanceof Time time) ? timeFormatter.format(time) : data.toString()) + "'";
            case "TIMESTAMP" ->
                    "TIMESTAMP '" + ((data instanceof Timestamp timestamp) ? timestampFormatter.format(new Date((timestamp).getTime())) : data.toString()) + "'";
            case "TEXT", "CHAR", "CHARACTER", "VARCHAR", "CHARACTER VARYING", "NCHAR", "NVARCHAR" ->
                    "'" + data.toString().replace("'", "''") + "'";
            case "BLOB", "BINARY", "VARBINARY" -> "HEXTORAW('" + DbUtils.handBinaryData(data) + "')";
            case "NCLOB", "CLOB" -> "'" + ((String) data).replace("'", "''") + "'";
            default -> data.toString();
        };
    }
}
