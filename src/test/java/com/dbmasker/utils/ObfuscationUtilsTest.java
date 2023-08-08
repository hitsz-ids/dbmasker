package com.dbmasker.utils;

import com.dbmasker.data.ObfuscationRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;

class ObfuscationUtilsTest {

    @Test
    void testMask() {
        String input = "abcdefghijklmnopqrstuvwxyz";
        int start = 5;
        int end = 10;
        char maskChar = '*';
        String expected = "abcde*****klmnopqrstuvwxyz";
        String result = ObfuscationUtils.mask(input, start, end, maskChar);
        Assertions.assertEquals(expected, result);

        input = "abcdefghijklmnopqrstuvwxyz";
        start = 5;
        end = 30;
        maskChar = '*';
        expected = "abcde*********************";
        result = ObfuscationUtils.mask(input, start, end, maskChar);
        Assertions.assertEquals(expected, result);

        input = "";
        start = 5;
        end = 10;
        maskChar = '*';
        expected = "";
        result = ObfuscationUtils.mask(input, start, end, maskChar);
        Assertions.assertEquals(expected, result);

        input = null;
        start = 5;
        end = 10;
        maskChar = '*';
        expected = null;
        result = ObfuscationUtils.mask(input, start, end, maskChar);
        Assertions.assertEquals(expected, result);
    }

    @Test
    void testTruncate() {
        Assertions.assertEquals("Hello", ObfuscationUtils.truncate("Hello, World!", 0, 5));
        Assertions.assertEquals("World", ObfuscationUtils.truncate("Hello, World!", 7, 12));
        Assertions.assertEquals("lo, Wor", ObfuscationUtils.truncate("Hello, World!", 3, 10));
        Assertions.assertEquals("Hello, World!", ObfuscationUtils.truncate("Hello, World!", 0, 13));
        Assertions.assertEquals("Hello, World!", ObfuscationUtils.truncate("Hello, World!", -1, 14));
        Assertions.assertEquals("", ObfuscationUtils.truncate("", 0, 5));
        Assertions.assertEquals(null, ObfuscationUtils.truncate(null, 0, 5));
    }

    @Test
    void testReplaceWithRegex() {
        String input = "abc123def456";
        String regex = "\\d+";
        String replacement = "***";
        Assertions.assertEquals("abc***def***", ObfuscationUtils.replaceWithRegex(input, regex, replacement));

        input = "test.email@example.com";
        regex = "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}\\b";
        replacement = "[email redacted]";
        Assertions.assertEquals("[email redacted]", ObfuscationUtils.replaceWithRegex(input, regex, replacement));

        input = "Please remove (123) 456-7890 and 098-765-4321";
        regex = "\\(?\\d{3}\\)?[-.\\s]?\\d{3}[-.\\s]?\\d{4}";
        replacement = "[phone redacted]";
        Assertions.assertEquals("Please remove [phone redacted] and [phone redacted]", ObfuscationUtils.replaceWithRegex(input, regex, replacement));

        input = "18678776621";
        regex = "(\\d{3})\\d{4}(\\d{4})";
        replacement = "$1****$2";
        Assertions.assertEquals("186****6621", ObfuscationUtils.replaceWithRegex(input, regex, replacement));

        input = "110111202011220026";
        regex = "(\\w{3})\\w*(\\w{4})";
        replacement = "$1****$2";
        Assertions.assertEquals("110****0026", ObfuscationUtils.replaceWithRegex(input, regex, replacement));
    }

    @Test
    void testGeneralize() {
        Assertions.assertEquals("20-29", ObfuscationUtils.generalize(25, 10));
        Assertions.assertEquals("40-44", ObfuscationUtils.generalize(42, 5));
        Assertions.assertEquals("0-99", ObfuscationUtils.generalize(0, 100));
    }

    @Test
    void testAddNoise() throws NoSuchAlgorithmException {
        double originalValue = 10.0;
        double noiseRange = 1.0;
        double noisyValue = ObfuscationUtils.addNoise(originalValue, noiseRange);

        Assertions.assertNotEquals(originalValue, noisyValue);
        Assertions.assertTrue(noisyValue >= (originalValue - noiseRange / 2));
        Assertions.assertTrue(noisyValue <= (originalValue + noiseRange / 2));
    }

    @Test
    void testDoObfuscation() throws NoSuchAlgorithmException {
        ObfuscationRule maskRule = new ObfuscationRule();
        maskRule.setMethod(ObfuscationMethod.MASK);
        maskRule.setStart(1);
        maskRule.setEnd(3);
        maskRule.setMaskChar('*');
        Object result = ObfuscationUtils.doObfuscation("hello", maskRule);
        Assertions.assertEquals("h**lo", result);

        // more test cases
        maskRule = new ObfuscationRule();
        maskRule.setMethod(ObfuscationMethod.MASK);
        Assertions.assertEquals("hello", ObfuscationUtils.doObfuscation("hello", maskRule));

        maskRule = new ObfuscationRule();
        Assertions.assertEquals("hello", ObfuscationUtils.doObfuscation("hello", maskRule));


        ObfuscationRule truncateRule = new ObfuscationRule();
        truncateRule.setMethod(ObfuscationMethod.TRUNCATE);
        truncateRule.setStart(0);
        truncateRule.setEnd(2);
        result = ObfuscationUtils.doObfuscation("hello", truncateRule);
        Assertions.assertEquals("he", result);

        // more test cases
        truncateRule = new ObfuscationRule();
        truncateRule.setMethod(ObfuscationMethod.TRUNCATE);
        Assertions.assertEquals("hello", ObfuscationUtils.doObfuscation("hello", maskRule));


        ObfuscationRule replaceRule = new ObfuscationRule();
        replaceRule.setMethod(ObfuscationMethod.REPLACE);
        replaceRule.setRegex("\\d");
        replaceRule.setReplacement("X");
        result = ObfuscationUtils.doObfuscation("a1b2c3", replaceRule);
        Assertions.assertEquals("aXbXcX", result);

        // more test cases
        replaceRule = new ObfuscationRule();
        replaceRule.setMethod(ObfuscationMethod.REPLACE);
        Assertions.assertEquals("hello", ObfuscationUtils.doObfuscation("hello", maskRule));

        ObfuscationRule generalizeRule = new ObfuscationRule();
        generalizeRule.setMethod(ObfuscationMethod.GENERALIZE);
        generalizeRule.setRange(10);
        result = ObfuscationUtils.doObfuscation(25, generalizeRule);
        Assertions.assertEquals("20-29", result);

        // more test cases
        generalizeRule = new ObfuscationRule();
        generalizeRule.setMethod(ObfuscationMethod.GENERALIZE);
        Assertions.assertEquals("hello", ObfuscationUtils.doObfuscation("hello", maskRule));

        ObfuscationRule addNoiseRule = new ObfuscationRule();
        addNoiseRule.setMethod(ObfuscationMethod.ADD_NOISE);
        addNoiseRule.setNoiseRange(0.5);
        result = ObfuscationUtils.doObfuscation(5.0, addNoiseRule);
        double expectedResult = (Double) result;
        Assertions.assertTrue(expectedResult >= 4.5 && expectedResult <= 5.5);

        // more test cases
        addNoiseRule = new ObfuscationRule();
        addNoiseRule.setMethod(ObfuscationMethod.ADD_NOISE);
        Assertions.assertEquals("hello", ObfuscationUtils.doObfuscation("hello", maskRule));
    }
}
