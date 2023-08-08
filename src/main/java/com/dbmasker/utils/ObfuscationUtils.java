package com.dbmasker.utils;

import com.dbmasker.data.ObfuscationRule;
import com.dbmasker.exception.RandomNoiseGenerationFailedException;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility class providing methods for obfuscating sensitive data.
 */
public class ObfuscationUtils {

    /**
     * Default constructor for ObfuscationUtils class.
     */
    private ObfuscationUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * Masks a portion of the input string with the specified mask character.
     *
     * @param input     The input string to be masked.
     * @param start     The starting index (inclusive) of the portion to be masked.
     * @param end       The ending index (exclusive) of the portion to be masked.
     * @param maskChar  The character used to mask the input string.
     * @return The masked string with the specified portion replaced by the mask character.
     */
    public static String mask(String input, int start, int end, char maskChar) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        int length = input.length();
        StringBuilder masked = new StringBuilder(input);

        for (int i = start; i < end && i < length; i++) {
            masked.setCharAt(i, maskChar);
        }

        return masked.toString();
    }

    /**
     * Truncates the input string by returning a substring of the original string.
     *
     * @param input The input string to be truncated.
     * @param start The starting index (inclusive) of the substring.
     * @param end   The ending index (exclusive) of the substring.
     * @return The substring from the input string or the original string if the indices are invalid.
     */
    public static String truncate(String input, int start, int end) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        int length = input.length();
        if (start < 0 || end < 0 || start >= length || end > length || start > end) {
            return input;
        }

        return input.substring(start, end);
    }

    /**
     * Replaces all occurrences of a pattern specified by the given regular expression with the given replacement string.
     *
     * @param input       The input string to be processed.
     * @param regex       The regular expression pattern to match.
     * @param replacement The replacement string to substitute for each match.
     * @return The resulting string with all matched occurrences replaced by the given replacement string.
     */
    public static String replaceWithRegex(String input, String regex, String replacement) {
        if (input == null || input.isEmpty() || regex == null || replacement == null) {
            return input;
        }

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);
        return matcher.replaceAll(replacement);
    }

    /**
     * Generalizes an integer value by grouping it into a range.
     * @param data The integer value to be generalized.
     * @param range The range size for the generalization.
     * @return A string representation of the generalized range in the format "lowerBound-upperBound".
     */
    public static String generalize(int data, int range) {
        int lowerBound = data / range * range;
        int upperBound = lowerBound + range - 1;

        return lowerBound + "-" + upperBound;
    }

    /**
     * Adds random noise to the given original value within the specified noise range.
     *
     * @param originalValue The original double value to which noise will be added.
     * @param noiseRange The range of the random noise to be added. The actual noise added
     *                   will be between -noiseRange/2 and +noiseRange/2.
     * @return The double value with added noise within the specified noise range.
     */
    public static double addNoise(double originalValue, double noiseRange) {
        try {
            Random random = SecureRandom.getInstanceStrong();
            double noise = random.nextDouble() * noiseRange - (noiseRange / 2);
            return originalValue + noise;
        } catch (NoSuchAlgorithmException e) {
            throw new RandomNoiseGenerationFailedException("Failed to generate random noise", e);
        }
    }

    /**
     * Performs data obfuscation based on the provided obfuscation rule.
     * @param data the data to be obfuscated.
     * @param obfuscationRule the obfuscation rule to be applied.
     * @return the obfuscated data.
     */
    public static Object doObfuscation(Object data, ObfuscationRule obfuscationRule) {
        ObfuscationMethod method = obfuscationRule.getMethod();
        if (method == null) {
            return data;
        }
        return switch (method) {
            case MASK ->
                    mask(data.toString(), obfuscationRule.getStart(), obfuscationRule.getEnd(), obfuscationRule.getMaskChar());
            case TRUNCATE -> truncate(data.toString(), obfuscationRule.getStart(), obfuscationRule.getEnd());
            case REPLACE ->
                    replaceWithRegex(data.toString(), obfuscationRule.getRegex(), obfuscationRule.getReplacement());
            case GENERALIZE -> generalize(Integer.parseInt(data.toString()), obfuscationRule.getRange());
            case ADD_NOISE -> addNoise(Double.parseDouble(data.toString()), obfuscationRule.getNoiseRange());
            default -> throw new IllegalArgumentException("Invalid obfuscation method");
        };
    }
}
