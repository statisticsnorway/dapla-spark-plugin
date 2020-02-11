package no.ssb.dapla.spark.plugin.pseudo;

public class PseudoException extends RuntimeException {
    public PseudoException(String message) {
        super(message);
    }

    public PseudoException(String message, Throwable cause) {
        super(message, cause);
    }
}
