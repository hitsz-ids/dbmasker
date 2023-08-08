package com.dbmasker.utils;

/**
 This is an enumeration representing the available obfuscation methods.
 */
public enum ObfuscationMethod {
    /**
     * Masking: a technique where certain parts of the data are replaced with a predefined mask.
     */
    MASK(1),
    /**
     * Truncation: a technique where the data is shortened to a certain length.
     */
    TRUNCATE(2),
    /**
     * Replacement: a technique where data is replaced with another value or string.
     */
    REPLACE(3),
    /**
     * Generalization: a technique where data is generalized to a higher level of abstraction.
     */
    GENERALIZE(4),
    /**
     * Adding noise: a technique where random values are added to the data to make it more difficult to interpret.
     */
    ADD_NOISE(5);

    private final int method;

    /**
     * Constructor for ObfuscationMethod enum.
     * @param method the code representing the obfuscation method.
     */
    ObfuscationMethod(int method) {
        this.method = method;
    }

    /**
     * Getter for the code representing the obfuscation method.
     * @return the code representing the obfuscation method.
     */
    public int getMethod() {
        return method;
    }

    /**
     * Method for getting the ObfuscationMethod based on the code representing the obfuscation method.
     * @param code the code representing the obfuscation method.
     * @return the ObfuscationMethod corresponding to the code provided.
     * @throws IllegalArgumentException if an invalid code is provided.
     */
    public static ObfuscationMethod valueOf(int code) {
        for (ObfuscationMethod obfuscationMethod : ObfuscationMethod.values()) {
            if (obfuscationMethod.getMethod() == code) {
                return obfuscationMethod;
            }
        }
        throw new IllegalArgumentException("Invalid code: " + code);
    }
}
