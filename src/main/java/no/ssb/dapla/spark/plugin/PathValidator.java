package no.ssb.dapla.spark.plugin;

import java.util.regex.Pattern;

public class PathValidator {

    private static Pattern readablePath = Pattern.compile("/(\\w|-|\\*|/)+");
    private static Pattern writablePath = Pattern.compile("/(\\w|-|/)+");

    public static void validateRead(String path) {
        if (!readablePath.matcher(path).matches()) {
            throw createValidationException(path);
        }
    }

    public static void validateWrite(String path) {
        if (!writablePath.matcher(path).matches()) {
            throw createValidationException(path);
        }
    }

    private static ValidationException createValidationException(String path) {
        return new ValidationException(String.format("Filstien '%s' er ikke gyldig. En filsti må starte med '/'" +
                " og kan ikke inneholde mellomrom eller spesialtegn.", path));
    }

    public static class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }
    }

}
