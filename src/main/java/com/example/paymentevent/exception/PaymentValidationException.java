package com.example.paymentevent.exception;

/**
 * Base type for business-rule failures discovered while processing a payment event
 * (as opposed to structural/format errors, which are rejected synchronously at the REST layer).
 * PaymentService.process() handles insufficient funds / currency mismatch / unknown account
 * itself (FAILED + payment.failed event, no exception escapes). The one subtype it still
 * throws is PaymentEventMismatchException, which is non-retryable and goes to the DLT.
 */
public abstract class PaymentValidationException extends RuntimeException {

    protected PaymentValidationException(String message) {
        super(message);
    }
}
