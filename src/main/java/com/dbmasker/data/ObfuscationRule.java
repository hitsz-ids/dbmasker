package com.dbmasker.data;

import com.dbmasker.utils.ObfuscationMethod;

/**
 This class represents an obfuscation rule, which defines how to obfuscate a portion of data.
 */
public class ObfuscationRule {

    private ObfuscationMethod method;
    private int start; // used for masking and truncation
    private int end; // used for masking and truncation
    private char maskChar; //used for masking
    private String regex; // used for replacement
    private String replacement; // used for replacement
    private int range; // used for generalization
    private double noiseRange; // used for adding noise

    /**
     * Constructor for ObfuscationRule.
     */
    public ObfuscationRule() {
        start = 0;
        end = 0;
        maskChar = '*';
        range = 0;
        regex = "";
        replacement = "";
        noiseRange = 0;
    }

    /**
     * Getter for the obfuscation method used in this rule.
     * @return the obfuscation method used in this rule.
     */
    public ObfuscationMethod getMethod() {
        return method;
    }

    /**
     * Setter for the obfuscation method used in this rule.
     * @param method the obfuscation method to be used in this rule.
     */
    public void setMethod(ObfuscationMethod method) {
        this.method = method;
    }

    /**
     * Getter for the starting index of the data portion to be obfuscated.
     * @return the starting index of the data portion to be obfuscated.
     */
    public int getStart() {
        return start;
    }

    /**
     * Setter for the starting index of the data portion to be obfuscated.
     * @param start the starting index of the data portion to be obfuscated.
     */
    public void setStart(int start) {
        this.start = start;
    }

    /**
     * Getter for the ending index (exclusive) of the data portion to be obfuscated.
     * @return the ending index (exclusive) of the data portion to be obfuscated.
     */
    public int getEnd() {
        return end;
    }

    /**
     * Setter for the ending index (exclusive) of the data portion to be obfuscated.
     * @param end the ending index (exclusive) of the data portion to be obfuscated.
     */
    public void setEnd(int end) {
        this.end = end;
    }

    /**
     * Getter for the regular expression used to identify the data portion to be obfuscated.
     * @return the regular expression used to identify the data portion to be obfuscated.
     */
    public String getRegex() {
        return regex;
    }

    /**
     * Setter for the regular expression used to identify the data portion to be obfuscated.
     * @param regex the regular expression used to identify the data portion to be obfuscated.
     */
    public void setRegex(String regex) {
        this.regex = regex;
    }

    /**
     * Getter for the replacement string used to obfuscate the identified data portion.
     * @return the replacement string used to obfuscate the identified data portion.
     */
    public String getReplacement() {
        return replacement;
    }

    /**
     * Setter for the replacement string used to obfuscate the identified data portion.
     * @param replacement the replacement string used to obfuscate the identified data portion.
     */
    public void setReplacement(String replacement) {
        this.replacement = replacement;
    }

    /**
     * Getter for the range used for generalizing the identified data portion.
     * @return the range used for generalizing the identified data portion.
     */
    public int getRange() {
        return range;
    }

    /**
     * Setter for the range used for generalizing the identified data portion.
     * @param range the range used for generalizing the identified data portion.
     */
    public void setRange(int range) {
        this.range = range;
    }

    /**
     * Getter for the noise range used for adding noise to the identified data portion.
     * @return the noise range used for adding noise to the identified* data portion.
     */
    public double getNoiseRange() {
        return noiseRange;
    }

    /**
     * Setter for the noise range used for adding noise to the identified data portion.
     * @param noiseRange the noise range used for adding noise to the identified data portion.
     */
    public void setNoiseRange(double noiseRange) {
        this.noiseRange = noiseRange;
    }

    /**
     * Getter for the masking character.
     * @return the masking character.
     */
    public char getMaskChar() {
        return maskChar;
    }

    /**
     * Setter for the masking character.
     * @param maskChar the character to be used for masking.
     */
    public void setMaskChar(char maskChar) {
        this.maskChar = maskChar;
    }

}