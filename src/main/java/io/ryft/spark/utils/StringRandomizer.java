package io.ryft.spark.utils;

import java.util.Random;
import java.util.stream.Collectors;

/**
 * Utility class for generating random alphanumeric strings.
 * <p>
 * This class provides methods to generate random strings composed of uppercase letters,
 * lowercase letters, and digits. this is meant to prevent any unnecessary dependencies that might collide with other dependencies.
 * </p>
 */
public class StringRandomizer {

    private static final String ALPHANUMERIC = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random RANDOM = new Random();
    private static final int MIN_LENGTH = 3;
    private static final int MAX_LENGTH = 10;

    /**
     * Generates a random alphanumeric string of the specified length.
     * <p>
     * The generated string will contain characters from the set: ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.
     * </p>
     *
     * @param length the length of the generated string
     * @return a random alphanumeric string of the specified length
     * @throws IllegalArgumentException if {@code length} is negative
     */
    public static String generateUniqueString(int length) {
        int boundedLength = restrictLengthToBounds(length);
        return RANDOM.ints(boundedLength, 0, ALPHANUMERIC.length())
                .mapToObj(ALPHANUMERIC::charAt)
                .map(Object::toString)
                .collect(Collectors.joining());
    }

    /**
     * Restricts the length within the predefined bounds.
     *
     * @param length the length to restrict
     * @return the length within the bounds of MIN_LENGTH and MAX_LENGTH
     */
    private static int restrictLengthToBounds(int length) {
        return Math.max(MIN_LENGTH, Math.min(MAX_LENGTH, length));
    }
}
