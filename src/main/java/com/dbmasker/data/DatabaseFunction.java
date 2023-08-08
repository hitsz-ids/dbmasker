package com.dbmasker.data;

import java.util.Objects;

/**
 * The DatabaseFunction class is used to store information related to database functions.
 */
public class DatabaseFunction {

    private String schemaName; //The name of the schema where the table is located.
    private String name; // The name of the function

    /**
     * Constructs a new DatabaseFunction object.
     *
     * @param schemaName The name of the schema where the table is located.
     * @param name       The name of the function
     */
    public DatabaseFunction(String schemaName, String name) {
        this.schemaName = schemaName;
        this.name = name;
    }

    /**
     * Getter for the schemaName character.
     * @return the schemaName character.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Setter for the schemaName character.
     * @param schemaName the character to be used for masking.
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * Getter for the name character.
     * @return the name character.
     */
    public String getName() {
        return name;
    }

    /**
     * Setter for the name character.
     * @param name the character to be used for masking.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the string representation of the DatabaseFunction object.
     *
     * @return The string representation containing the function name and return type
     */
    @Override
    public String toString() {
        return "DatabaseFunction{" +
                "name='" + name + '\'' +
                ", schemaName='" + schemaName + '\'' +
                '}';
    }

    /**
     * Determines if this DatabaseFunction object is equal to the given object.
     * Two DatabaseFunction objects are considered equal if they have the same name
     * and return type.
     *
     * @param o The object to compare with this DatabaseFunction object.
     * @return true if the given object is equal to this DatabaseFunction object, otherwise false.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatabaseFunction that)) return false;
        return Objects.equals(getName(), that.getName())
                && Objects.equals(getSchemaName(), that.getSchemaName());
    }

    /**
     * Generates a hash code for this DatabaseFunction object based on the name
     * and return type.
     *
     * @return the generated hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hash(getSchemaName(), getName());
    }
}

