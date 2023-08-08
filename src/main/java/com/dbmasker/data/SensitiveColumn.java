package com.dbmasker.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This class represents a sensitive column in a database table.
 */
public class SensitiveColumn {

    private String schemaName; // The name of the schema where the table is located.
    private String tableName; // The name of the table containing the sensitive column
    private String columnName; // The name of the sensitive column
    private String regex; // The regular expression used to match sensitive data
    /**
     * A list of data values that match the specified criteria for this sensitive column,
     * stores up to MATCH_DATA_SIZE(default value is 5) records
     */
    private List<Object> matchData;

    /**
     * Constructor
     *
     * @param schemaName The name of the schema where the table is located.
     * @param tableName   The name of the table containing the sensitive column
     * @param columnName  The name of the sensitive column
     * @param regex       The regular expression used to match sensitive data
     */
    public SensitiveColumn(String schemaName, String tableName, String columnName, String regex) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.regex = regex;
        this.matchData = new ArrayList<>();
    }

    /**
     * Constructor for the SensitiveColumn class.
     *
     * @param schemaName The name of the schema where the table is located.
     * @param tableName   The name of the table containing the sensitive column
     * @param columnName  The name of the sensitive column
     * @param regex       The regular expression used to match sensitive data
     * @param matchData   A list of data values that match the specified criteria for this sensitive column,
     *                    stores up to MATCH_DATA_SIZE(default value is 5) records
     */
    public SensitiveColumn(String schemaName, String tableName, String columnName, String regex, List<Object> matchData) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.regex = regex;
        this.matchData = matchData;
    }

    /**
     Getter for the schemaName character.
     @return the schemaName character.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     Setter for the schemaName character.
     @param schemaName the character to be used for masking.
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     Getter for the tableName character.
     @return the tableName character.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     Setter for the tableName character.
     @param tableName the character to be used for masking.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     Getter for the columnName character.
     @return the columnName character.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     Setter for the columnName character.
     @param columnName the character to be used for masking.
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     Getter for the regex character.
     @return the regex character.
     */
    public String getRegex() {
        return regex;
    }

    /**
     Setter for the regex character.
     @param regex the character to be used for masking.
     */
    public void setRegex(String regex) {
        this.regex = regex;
    }

    /**
     Getter for the matchData character.
     @return the matchData character.
     */
    public List<Object> getMatchData() {
        return matchData;
    }

    /**
     * Setter for the matchData character.
     * @param matchData the character to be used for masking.
     */
    public void setMatchData(List<Object> matchData) {
        this.matchData = matchData;
    }

    /**
     * Determines if this SensitiveColumn object is equal to the given object.
     * Two SensitiveColumn objects are considered equal if they have the same name
     * and return type.
     *
     * @param o The object to compare with this SensitiveColumn object.
     * @return true if the given object is equal to this SensitiveColumn object, otherwise false.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SensitiveColumn that)) return false;
        return Objects.equals(getColumnName(), that.getColumnName())
                && Objects.equals(getRegex(), that.getRegex())
                && Objects.equals(getSchemaName(), that.getSchemaName())
                && Objects.equals(getTableName(), that.getTableName())
                && Objects.equals(getMatchData(), that.getMatchData());
    }

    /**
     * Generates a hash code for this SensitiveColumn object based on the name
     * and return type.
     *
     * @return the generated hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hash(getColumnName(), getRegex(), getSchemaName(), getTableName(), getMatchData());
    }
}
