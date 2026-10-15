package com.example.paymentevent.exception;

/**
 * A payment.screening.completed payload that is not JSON or does not match the v1 schema.
 * Non-retryable: the same bytes will never become valid, so it goes straight to the DLT.
 */
public class InvalidScreeningEventException extends RuntimeException {

    public InvalidScreeningEventException(String message) {
        super(message);
    }

    public InvalidScreeningEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
