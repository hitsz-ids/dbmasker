package com.dbmasker.data;

import java.util.Objects;

/**
 * This class represents the metadata of a table, including table name, column name, data type, and column size.
 */
public class TableMetaData {

    private String tableName;
    private String columnName;
    private String dataType;
    private String columnSize;

    /**
     * Constructs a new TableMetaData instance with the given table name, column name, data type, and column size.
     *
     * @param tableName  the name of the table.
     * @param columnName the name of the column.
     * @param dataType   the data type of the column.
     * @param columnSize the size of the column.
     */
    public TableMetaData(String tableName, String columnName, String dataType, String columnSize) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
        this.columnSize = columnSize;
    }

    /**
     * Returns the table name.
     *
     * @return the name of the table.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the column name.
     *
     * @return the name of the column.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Returns the data type of the column.
     *
     * @return the data type of the column.
     */
    public String getDataType() {
        return dataType;
    }

    /**
     * Returns the column size.
     *
     * @return the size of the column.
     */
    public String getColumnSize() {
        return columnSize;
    }

    /**
     * Returns a string representation of the TableMetaData instance.
     *
     * @return a string representation of the TableMetaData instance.
     */
    @Override
    public String toString() {
        return "TableMetaData{" +
                "tableName='" + tableName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", dataType='" + dataType + '\'' +
                ", columnSize='" + columnSize + '\'' +
                '}';
    }

    /**
     * Determines if this TableMetaData object is equal to the given object.
     * Two TableMetaData objects are considered equal if they have the same table name,
     * column name, data type, and column size.
     *
     * @param o The object to compare with this TableMetaData object.
     * @return true if the given object is equal to this TableMetaData object, otherwise false.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableMetaData that)) return false;
        return Objects.equals(getTableName(), that.getTableName())
                && Objects.equals(getColumnName(), that.getColumnName())
                && Objects.equals(getDataType(), that.getDataType())
                && Objects.equals(getColumnSize(), that.getColumnSize());
    }

    /**
     * Generates a hash code for this TableMetaData object based on the table name,
     * column name, data type, and column size.
     *
     * @return the generated hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hash(getTableName(), getColumnName(), getDataType(), getColumnSize());
    }
}
