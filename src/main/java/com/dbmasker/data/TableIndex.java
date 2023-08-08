package com.dbmasker.data;

import java.util.Objects;

/**
 * The TableIndex class represents an index in a database table.
 */
public class TableIndex {

    private String indexName;
    private String columnName;
    private boolean unique;

    /**
     * Constructs a new TableIndex with the specified index name, column name, and uniqueness.
     *
     * @param indexName  the name of the index.
     * @param columnName the name of the column in the index.
     * @param unique     true if the index is unique, false otherwise.
     */
    public TableIndex(String indexName, String columnName, boolean unique) {
        this.indexName = indexName;
        this.columnName = columnName;
        this.unique = unique;
    }

    /**
     * Returns the name of the index.
     *
     * @return the name of the index.
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Returns the name of the column in the index.
     *
     * @return the name of the column in the index.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Returns true if the index is unique, false otherwise.
     *
     * @return true if the index is unique, false otherwise.
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Returns a string representation of the TableIndex object.
     *
     * @return a string representation of the TableIndex object.
     */
    @Override
    public String toString() {
        return "TableIndex{" +
                "indexName='" + indexName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", unique=" + unique +
                '}';
    }

    /**
     * Determines if this TableIndex object is equal to the given object.
     * Two TableIndex objects are considered equal if they have the same index name,
     * column name, and unique flag.
     *
     * @param o The object to compare with this TableIndex object.
     * @return true if the given object is equal to this TableIndex object, otherwise false.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableIndex that)) return false;
        return isUnique() == that.isUnique() && Objects.equals(getIndexName(), that.getIndexName())
                && Objects.equals(getColumnName(), that.getColumnName());
    }

    /**
     * Generates a hash code for this TableIndex object based on the index name,
     * column name, and unique flag.
     *
     * @return the generated hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hash(getIndexName(), getColumnName(), isUnique());
    }
}
