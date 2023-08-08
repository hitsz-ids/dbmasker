package com.dbmasker.dialect;

import com.dbmasker.utils.DbUtils;

/**
 * SQLiteDialect class implements the Dialect interface for SQLite.
 */
public class SQLiteDialect extends BaseDialect {

    /**
     * Default constructor for SQLiteDialect class.
     */
    public SQLiteDialect() {
        super();
    }

    /**
     * Formats the provided data object into a string suitable for an SQLite query,
     * based on the provided SQLite data type.
     * If the data object is null, it will return "NULL". For INTEGER and REAL data types,
     * it will return the string representation of the data object. For TEXT, it returns the string
     * representation of the data object, with single quotes replaced with two single quotes.
     * For BLOB, it returns the hexadecimal representation of the binary data.
     *
     * @param data The data object that needs to be formatted.
     * @param type The SQLite data type of the data object (e.g., "NULL", "INTEGER", "REAL", "TEXT", "BLOB").
     * @return A string that represents the data object in a format suitable for an SQLite query.
     * @throws IllegalArgumentException If the provided SQLite data type is unknown.
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
            case "NULL" -> {
                return "NULL";
            }
            // SQLite accepts integers as is.
            // SQLite accepts real (floating point) numbers as is.
            case "TEXT" -> {
                // Strings need to be quoted and escaped.
                String text = data.toString();
                text = text.replace("'", "''");  // Replace single quotes with two single quotes.
                return "'" + text + "'";
            }
            case "BLOB" -> {
                // Binary data needs to be converted to an appropriate format.
                return "X'" + DbUtils.handBinaryData(data) +
                        "'";
            }
            default -> {
                return data.toString();
            }
        }
    }

}
