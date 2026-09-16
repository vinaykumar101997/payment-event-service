package com.example.paymentevent.exception;

/**
 * Base type for business-rule failures discovered while processing a payment event
 * (as opposed to structural/format errors, which are rejected synchronously at the REST layer).
 * Non-retryable: Kafka's error handler routes these straight to the DLT.
 */
public abstract class PaymentValidationException extends RuntimeException {

    protected PaymentValidationException(String message) {
        super(message);
    }
}
