package com.dbmasker.dialect;


import com.dbmasker.utils.DbUtils;

/**
 * MsSQLDialect class implements the Dialect interface for MsSQL.
 */
public class MsSQLDialect extends BaseDialect {


    /**
     * Default constructor for MsSQLDialect class.
     */
    public MsSQLDialect() {
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
        switch (typeUpper) {
            // For character types, escape single quotes and return string wrapped in quotes. Prefix with N for unicode strings.
            case "CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR", "NTEXT" -> {
                String text = data.toString();
                text = text.replace("'", "''");  // Replace single quotes with two single quotes.
                return "N'" + text + "'";  // Prefix with N for unicode strings
            }
            case "DATE", "DATETIME", "DATETIME2", "SMALLDATETIME", "TIME", "DATETIMEOFFSET" -> {
                // For date and time types, if the data is already a string, quote it directly, otherwise format it.
                if (data instanceof String stringData) {
                    return "'" + stringData.replace("'", "''") + "'";
                } else {
                    // For date and time types, assume data is a java.util.Date or java.sql.Timestamp object and format it accordingly
                    java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    return "'" + sdf.format(data) + "'";
                }
            }
            case "BINARY", "VARBINARY", "IMAGE" -> {
                // For binary types, assume data is either a byte array or a string and convert it to a hex string
                return "0x" + DbUtils.handBinaryData(data);
            }
            case "BIT", "BIGINT", "INT", "SMALLINT", "TINYINT", "MONEY", "SMALLMONEY", "NUMERIC", "DECIMAL", "REAL", "FLOAT" -> {
                return data.toString();  // For numeric types, use toString
            }
            // For XML, assume data is a string containing valid XML and quote it. For geometry and geography types,
            // assume data is a string containing a WKT representation of the geometry.
            default -> {
                // For other types, use the default toString() handling
                return "'" + data.toString().replace("'", "''") + "'";
            }
        }
    }
}
