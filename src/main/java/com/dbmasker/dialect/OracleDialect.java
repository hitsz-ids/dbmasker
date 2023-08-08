package com.dbmasker.dialect;


import com.dbmasker.utils.DbUtils;

/**
 * OracleDialect class implements the Dialect interface for Oracle.
 */
public class OracleDialect extends BaseDialect {

    /**
     * Default constructor for OracleDialect class.
     */
    public OracleDialect() {
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

        if (typeUpper.startsWith("NUMBER") || typeUpper.equals("FLOAT") || typeUpper.equals("INTEGER")
                || typeUpper.equals("SMALLINT") || typeUpper.equals("REAL")
                || typeUpper.equals("DOUBLE PRECISION") || typeUpper.equals("DECIMAL")) {
            return data.toString();
        }

        if (typeUpper.equals("CHAR") || typeUpper.startsWith("VARCHAR2") || typeUpper.equals("LONG")) {
            String text = data.toString();
            text = text.replace("'", "''");  // Replace single quotes with two single quotes.
            return "'" + text + "'";
        }

        if (typeUpper.equals("CLOB")) {
            String text = data.toString();
            text = text.replace("'", "''");  // Replace single quotes with two single quotes.
            return "TO_CLOB('" + text + "')";
        }

        if (typeUpper.startsWith("XMLTYPE") || typeUpper.startsWith("SYS.XMLTYPE")) {
            String text = data.toString();
            text = text.replace("'", "''");  // Replace single quotes with two single quotes.
            return "XMLTYPE('" + text + "')";
        }

        if (typeUpper.startsWith("RAW") || typeUpper.equals("LONG RAW") || typeUpper.equals("BLOB")) {
            // For binary types, assume data is either a byte array or a string and convert it to a hex string
            return "HEXTORAW('" + DbUtils.handBinaryData(data) + "')";
        }

        if (typeUpper.equals("ROWID")) {
            String text = data.toString();
            return "'" + text + "'";
        }

        if (typeUpper.equals("DATE")) {
            String text = data.toString();
            // Assume the date is in the format 'yyyy-MM-dd'
            return "TO_DATE('" + text + "', 'yyyy-mm-dd')";
        }

        if (typeUpper.startsWith("TIMESTAMP")) {
            String text = data.toString();
            // Assume the timestamp is in the format 'yyyy-MM-dd HH:mm:ss'
            return "TIMESTAMP '" + text + "'";
        }

        if (typeUpper.startsWith("INTERVAL YEAR")) {
            String text = data.toString();
            // Assume the interval is in the format 'PnYnM'
            return "INTERVAL '" + text + "' YEAR TO MONTH";
        }

        if (typeUpper.startsWith("INTERVAL DAY")) {
            String text = data.toString();
            // Assume the interval is in the format 'PnDTnHnMnS'
            return "INTERVAL '" + text + "' DAY TO SECOND";
        }

        // For other types, use the default toString() handling
        return data.toString();
    }
}
