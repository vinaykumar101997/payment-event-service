package com.example.paymentevent.exception;

/** A list query with an unsupported status, a bad limit or overdue value, or an unknown cursor (400). */
public class InvalidQueryException extends RuntimeException {

    public InvalidQueryException(String message) {
        super(message);
    }
}
