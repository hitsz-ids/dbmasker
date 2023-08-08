package com.dbmasker.exception;

/**
 * This class represents an exception that is thrown when random noise generation fails.
 * It extends RuntimeException, and hence is an unchecked exception.
 */
public class RandomNoiseGenerationFailedException extends RuntimeException {

    /**
     * Constructs a new RandomNoiseGenerationFailedException with a specified detail message and cause.
     *
     * @param message The detail message, which is saved for later retrieval by the Throwable.getMessage() method.
     * @param cause The cause, which is saved for later retrieval by the Throwable.getCause() method.
     *              A null value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public RandomNoiseGenerationFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
