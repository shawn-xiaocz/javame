package io.javame.service;

/** Caller-safe business failure translated by both protocol adapters. */
public class MatchingException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String code;

    public MatchingException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
